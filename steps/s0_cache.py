"""
Local DuckDB cache layer — individual source tables + allocation result cache.

Optimisations in this version vs original
──────────────────────────────────────────
Opt 1  — precompute_allocation() is now the default path in main.py.
         Run all 3 allocation JOINs ONCE for all users; each chunk then does
         a fast indexed scan on _alloc_precomputed instead of 2 full JOINs.
         Expected speedup: ~10 h → ~20 min for 635 chunks.

Opt 4  — fetch_all_completion() fetches ALL users' completion in one DuckDB
         query (learning_activities + facilitator_learning_activities are
         already cached locally).  main.py merges once at the end rather
         than firing one query per chunk.

Opt 6  — Hash-based cache invalidation (CACHE_INVALIDATION_STRATEGY=hash).
         CRC32 of sorted (id, updated_at) for key allocation tables —
         far more precise than row count; avoids false invalidations from
         unrelated row inserts.  Falls back to row_count strategy when
         the env var is set to 'row_count'.

Opt 10 — Auto-checkpoint: saves the last successfully written chunk number
         to cache_meta so a killed run can auto-resume without --start-chunk.

Layer 1 — TableCache
  Caches every source table (except learning_activities /
  facilitator_learning_activities) into cache.duckdb.  s1_users and
  s2_allocation query DuckDB locally; s3_completion also queries DuckDB
  after refresh_completion_tables() caches those tables too.

Layer 2 — AllocationCache
  After s2 / precompute runs, the full allocation result is materialised as
  _alloc_precomputed in DuckDB.  Subsequent runs load chunks from there
  (sub-second indexed scan) instead of re-running the JOINs.

Cache file: cache.duckdb  (pipeline root, gitignored)
Use --force-refresh to bypass both layers and rebuild from scratch.
"""

import hashlib
import logging
import os
import re
import time
from datetime import datetime, timezone
from typing import List, Optional

import duckdb
import pandas as pd

from config import CACHE_INVALIDATION_STRATEGY, SOURCE_DB
from db import fetch

log = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _ensure_varchar_nulls(df: pd.DataFrame) -> pd.DataFrame:
    """
    Force all-NULL object columns to pandas StringDtype before registering
    with DuckDB so that DuckDB uses VARCHAR instead of inferring INT32.
    """
    df = df.copy()
    for col in df.columns:
        if df[col].dtype == object and df[col].isna().all():
            df[col] = pd.array([pd.NA] * len(df), dtype=pd.StringDtype())
    return df


_PIPELINE_DIR      = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_CACHE_PATH = os.path.join(_PIPELINE_DIR, "cache.duckdb")

# Tables whose state is checked to decide whether to invalidate the cache.
ALLOCATION_WATCH_TABLES = [
    "centre_subject",
    "batch_subject",
    "subject_trade",
    "ple_career_paths",
    "subject_ple_career_path",
    "ple_career_path_user",
    "subjects",
    "lessons",
]

# Columns used for hash computation per table.  We hash the sorted values of
# the primary key + updated_at (or created_at) so that only real data changes
# (not unrelated new rows) trigger a cache bust.
_HASH_COLUMNS: dict[str, list[str]] = {
    "centre_subject":          ["id", "centre_id", "subject_id"],
    "batch_subject":           ["id", "batch_id",  "subject_id"],
    "subject_trade":           ["id", "subject_id","trade_id"],
    "ple_career_paths":        ["id", "deleted_at"],
    "subject_ple_career_path": ["id", "ple_career_path_id", "subject_id"],
    "ple_career_path_user":    ["id", "user_id", "job_type_id", "status", "deleted_at"],
    "subjects":                ["id", "status", "deleted_at"],
    "lessons":                 ["id", "status", "deleted_at"],
}


# ─────────────────────────────────────────────────────────────────────────────
# Opt 6 — Hash-based invalidation helpers
# ─────────────────────────────────────────────────────────────────────────────

def _table_hash(table: str) -> str:
    """
    Compute a stable MD5 hash over the key columns of a watch table.
    Fetches only the hash-relevant columns so it's cheap over SSH.
    Falls back to row count if the table has no _HASH_COLUMNS entry.
    """
    cols = _HASH_COLUMNS.get(table)
    if not cols:
        # Fallback: just count rows (original behaviour for unlisted tables)
        row = fetch(SOURCE_DB, f"SELECT COUNT(*) AS cnt FROM {table}", None)
        return str(int(row["cnt"].iloc[0]))

    cols_sql = ", ".join(f"`{c}`" for c in cols)
    df = fetch(SOURCE_DB, f"SELECT {cols_sql} FROM {table} ORDER BY {cols[0]}", None)

    # Concatenate all values into a single string, then MD5.
    flat = "|".join(
        ",".join("" if pd.isna(v) else str(v) for v in row)
        for row in df.itertuples(index=False, name=None)
    )
    return hashlib.md5(flat.encode()).hexdigest()  # nosec — not a security hash


# ─────────────────────────────────────────────────────────────────────────────
# AllocationCache
# ─────────────────────────────────────────────────────────────────────────────

class AllocationCache:
    """
    Two responsibilities:
      1. Detect whether allocation source data changed (hash or row-count).
      2. Store / load the fully combined allocation result (allocation_cache table).
    """

    def __init__(self, path: str = DEFAULT_CACHE_PATH):
        self.path = path
        self._con = duckdb.connect(path)
        self._apply_memory_limit()
        self._init_schema()

    def _apply_memory_limit(self):
        """
        Cap DuckDB's buffer pool so it spills to disk instead of consuming
        ~80% of RAM (its default), which left no headroom for the in-memory
        completion DataFrame + parallel workers and led to OOM kills.
        """
        try:
            from config import DUCKDB_MEMORY_LIMIT
            self._con.execute(f"PRAGMA memory_limit='{DUCKDB_MEMORY_LIMIT}'")
            # temp_directory enables on-disk spilling of large intermediates.
            tmp = os.path.join(os.path.dirname(self.path) or ".", "duckdb_tmp")
            self._con.execute(f"PRAGMA temp_directory='{tmp}'")
            log.info("[cache] DuckDB memory_limit=%s, temp_directory=%s",
                     DUCKDB_MEMORY_LIMIT, tmp)
        except Exception as exc:
            log.warning("[cache] Could not set DuckDB memory limit: %s", exc)

    def _init_schema(self):
        self._con.execute("""
            CREATE TABLE IF NOT EXISTS cache_meta (
                key        VARCHAR PRIMARY KEY,
                int_value  BIGINT,
                str_value  VARCHAR,
                updated_at TIMESTAMP
            )
        """)
        # Row count snapshot table (kept for row_count strategy fallback)
        self._con.execute("""
            CREATE TABLE IF NOT EXISTS allocation_row_counts (
                table_name VARCHAR PRIMARY KEY,
                row_count  BIGINT,
                snapped_at TIMESTAMP
            )
        """)
        # Opt 6 — hash snapshot table
        self._con.execute("""
            CREATE TABLE IF NOT EXISTS allocation_hashes (
                table_name VARCHAR PRIMARY KEY,
                hash_value VARCHAR,
                snapped_at TIMESTAMP
            )
        """)

    # ── Allocation change detection ───────────────────────────────────────────

    def allocation_changed(self) -> bool:
        """
        Return True if any watch table has changed since the last snapshot,
        or if no snapshot exists yet.

        Strategy is controlled by CACHE_INVALIDATION_STRATEGY (config.py):
          'hash'      — MD5 of key columns; precise, no false positives.
          'row_count' — original COUNT(*) check; faster but coarser.
        """
        if CACHE_INVALIDATION_STRATEGY == "hash":
            return self._allocation_changed_hash()
        return self._allocation_changed_row_count()

    def _allocation_changed_hash(self) -> bool:
        existing = self._con.execute(
            "SELECT table_name, hash_value, snapped_at FROM allocation_hashes"
        ).fetchdf()

        if existing.empty:
            log.info("[cache] No hash snapshot found — first run, fetching live allocation")
            return True

        snapshot   = dict(zip(existing["table_name"], existing["hash_value"]))
        snapped_at = existing.set_index("table_name")["snapped_at"].to_dict()
        changed    = False
        changed_tbl = None

        log.info("[cache] ── Cache status (hash check) ──────────────────────────────")
        col_w = max(len(t) for t in ALLOCATION_WATCH_TABLES) + 2

        for table in ALLOCATION_WATCH_TABLES:
            try:
                current  = _table_hash(table)
                stored   = snapshot.get(table)
                snapped  = snapped_at.get(table, "unknown")

                if stored is None or current != stored:
                    log.info(
                        "[cache]   %-*s  CHANGED   (snapped: %s)",
                        col_w, table, snapped,
                    )
                    if not changed:
                        changed_tbl = table
                    changed = True
                else:
                    log.info(
                        "[cache]   %-*s  ok        (snapped: %s)",
                        col_w, table, snapped,
                    )
            except Exception as exc:
                log.warning("[cache]   %-*s  ERROR %s — treating as changed", col_w, table, exc)
                changed     = True
                changed_tbl = table

        if changed:
            log.info("[cache] ── Result: CHANGED (trigger: %s) — full allocation refresh ─", changed_tbl)
        else:
            log.info("[cache] ── Result: ALL CACHED ✓ — allocation will load from DuckDB ─")
        log.info("[cache] ─────────────────────────────────────────────────────────")
        return changed

    def _allocation_changed_row_count(self) -> bool:
        """Original row-count based detection (fallback / opt-out path)."""
        existing = self._con.execute(
            "SELECT table_name, row_count, snapped_at FROM allocation_row_counts"
        ).fetchdf()

        if existing.empty:
            log.info("[cache] No row-count snapshot found — first run, fetching live allocation")
            return True

        snapshot    = dict(zip(existing["table_name"], existing["row_count"]))
        snapped_at  = existing.set_index("table_name")["snapped_at"].to_dict()
        changed     = False
        changed_tbl = None

        log.info("[cache] ── Cache status (row_count check) ──────────────────────")
        col_w = max(len(t) for t in ALLOCATION_WATCH_TABLES) + 2

        for table in ALLOCATION_WATCH_TABLES:
            try:
                row     = fetch(SOURCE_DB, f"SELECT COUNT(*) AS cnt FROM {table}", None)
                current = int(row["cnt"].iloc[0])
                stored  = snapshot.get(table)
                snapped = snapped_at.get(table, "unknown")

                if stored is None or current != stored:
                    log.info(
                        "[cache]   %-*s  CHANGED   stored=%-8s  current=%-8d  (snapped: %s)",
                        col_w, table, stored, current, snapped,
                    )
                    if not changed:
                        changed_tbl = table
                    changed = True
                else:
                    log.info(
                        "[cache]   %-*s  ok        rows=%-8d               (snapped: %s)",
                        col_w, table, current, snapped,
                    )
            except Exception as exc:
                log.warning("[cache]   %-*s  ERROR %s — treating as changed", col_w, table, exc)
                changed     = True
                changed_tbl = table

        if changed:
            log.info("[cache] ── Result: CHANGED (trigger: %s) ─────────────────────", changed_tbl)
        else:
            log.info("[cache] ── Result: ALL CACHED ✓ ───────────────────────────────")
        log.info("[cache] ─────────────────────────────────────────────────────────")
        return changed

    def save_snapshot(self):
        """
        Save the current state snapshot for all watch tables.
        Saves both hash and row-count so either strategy can be used on
        the next run without losing the other's snapshot.
        """
        now    = datetime.now(timezone.utc).replace(tzinfo=None)
        saved  = 0

        for table in ALLOCATION_WATCH_TABLES:
            try:
                # Hash snapshot (opt 6)
                h = _table_hash(table)
                self._con.execute(
                    """
                    INSERT INTO allocation_hashes (table_name, hash_value, snapped_at)
                    VALUES (?, ?, ?)
                    ON CONFLICT (table_name) DO UPDATE SET
                        hash_value = excluded.hash_value,
                        snapped_at = excluded.snapped_at
                    """,
                    [table, h, now],
                )

                # Row count snapshot (kept for fallback)
                row   = fetch(SOURCE_DB, f"SELECT COUNT(*) AS cnt FROM {table}", None)
                count = int(row["cnt"].iloc[0])
                self._con.execute(
                    """
                    INSERT INTO allocation_row_counts (table_name, row_count, snapped_at)
                    VALUES (?, ?, ?)
                    ON CONFLICT (table_name) DO UPDATE SET
                        row_count  = excluded.row_count,
                        snapped_at = excluded.snapped_at
                    """,
                    [table, count, now],
                )
                saved += 1
            except Exception as exc:
                log.warning("[cache] Could not snapshot %s: %s", table, exc)

        log.info("[cache] Snapshot saved (%d / %d tables, strategy=%s)",
                 saved, len(ALLOCATION_WATCH_TABLES), CACHE_INVALIDATION_STRATEGY)

    # Keep old name as alias so any external callers don't break
    def save_row_count_snapshot(self):
        self.save_snapshot()

    # ── Allocation data cache ─────────────────────────────────────────────────

    def is_ready(self) -> bool:
        """True if the allocation cache table exists and has been finalised."""
        try:
            row = self._con.execute(
                "SELECT int_value FROM cache_meta WHERE key = 'allocation_total_rows'"
            ).fetchone()
            return row is not None and row[0] > 0
        except Exception:
            return False

    def reset(self):
        """Drop the allocation cache table and its completion marker."""
        self._con.execute("DROP TABLE IF EXISTS allocation_cache")
        self._con.execute("DELETE FROM cache_meta WHERE key = 'allocation_total_rows'")
        log.info("[cache] Allocation cache cleared — will rebuild this run")

    def append(self, df: pd.DataFrame):
        """Append one chunk of allocation data to the cache."""
        exists = self._con.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'allocation_cache'"
        ).fetchone()[0]
        if not exists:
            self._con.register("_chunk", _ensure_varchar_nulls(df))
            self._con.execute("CREATE TABLE allocation_cache AS SELECT * FROM _chunk")
            log.info("[cache] allocation_cache table created from first chunk")
        else:
            self._con.register("_chunk", df)
            self._con.execute("INSERT INTO allocation_cache SELECT * FROM _chunk")
        self._con.unregister("_chunk")

    def finalise(self, total_rows: int):
        """Mark the cache as complete and store the total row count."""
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        self._con.execute(
            """
            INSERT INTO cache_meta (key, int_value, str_value, updated_at)
            VALUES ('allocation_total_rows', ?, NULL, ?)
            ON CONFLICT (key) DO UPDATE SET
                int_value  = excluded.int_value,
                updated_at = excluded.updated_at
            """,
            [total_rows, now],
        )
        log.info("[cache] Allocation cache finalised — %d total rows stored", total_rows)

    def load_chunk(self, user_ids: list) -> pd.DataFrame:
        """Return allocation rows for the given user_ids from the local cache."""
        if not user_ids:
            return pd.DataFrame()
        placeholders = ", ".join(["?" for _ in user_ids])
        # Cursor per call — read concurrently by CHUNK_WORKERS threads.
        cur = self._con.cursor()
        try:
            return cur.execute(
                f"SELECT * FROM allocation_cache WHERE user_id IN ({placeholders})",
                user_ids,
            ).fetchdf()
        finally:
            cur.close()

    # ── Opt 10 — Auto-checkpoint ──────────────────────────────────────────────

    def save_checkpoint(self, chunk_idx: int) -> None:
        """Persist the last successfully written chunk index to cache_meta."""
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        self._con.execute(
            """
            INSERT INTO cache_meta (key, int_value, str_value, updated_at)
            VALUES ('last_written_chunk', ?, NULL, ?)
            ON CONFLICT (key) DO UPDATE SET
                int_value  = excluded.int_value,
                updated_at = excluded.updated_at
            """,
            [chunk_idx, now],
        )

    def load_checkpoint(self) -> Optional[int]:
        """
        Return the last successfully written chunk index, or None if no
        checkpoint exists (i.e. fresh run).
        """
        try:
            row = self._con.execute(
                "SELECT int_value FROM cache_meta WHERE key = 'last_written_chunk'"
            ).fetchone()
            return int(row[0]) if row and row[0] else None
        except Exception:
            return None

    def clear_checkpoint(self) -> None:
        """Delete the checkpoint after a successful full run."""
        self._con.execute(
            "DELETE FROM cache_meta WHERE key = 'last_written_chunk'"
        )

    def close(self):
        self._con.close()


# ─────────────────────────────────────────────────────────────────────────────
# Source tables cached into DuckDB
# ─────────────────────────────────────────────────────────────────────────────

SOURCE_CACHE_TABLES = [
    "users",
    "student_details",
    "subjects",
    "lessons",
    "lesson_types",
    "trades",
    "centre_subject",
    "batch_subject",
    "subject_trade",
    "ple_career_paths",
    "subject_ple_career_path",
    "ple_career_path_user",
]


class TableCache:
    """
    Caches individual source tables from quest_rearch_production into DuckDB.

    Once cached, s1_users, s2_allocation, and s3_completion all query DuckDB
    locally via make_fetch_fn() — no SSH tunnel needed for those steps.

    Opt 1 — precompute_allocation() runs all 3 allocation JOINs once for
             all users and stores the result as _alloc_precomputed.  Each
             chunk then does a fast indexed scan instead of 2 full JOINs.

    Opt 4 — fetch_all_completion() queries both completion tables in one shot
             for all users, returning a single DataFrame.  main.py merges
             once at the end rather than fetching per chunk.
    """

    def __init__(self, con: duckdb.DuckDBPyConnection):
        self._con = con
        self._con.execute("""
            CREATE TABLE IF NOT EXISTS source_table_meta (
                table_name  VARCHAR PRIMARY KEY,
                row_count   BIGINT,
                cached_at   TIMESTAMP
            )
        """)

    def is_fresh(self) -> bool:
        """
        True only when every source table has metadata AND actually exists
        as a table in DuckDB.
        """
        try:
            row = self._con.execute("""
                SELECT COUNT(*) AS n
                FROM   source_table_meta
                WHERE  table_name IN ({})
            """.format(", ".join(f"'{t}'" for t in SOURCE_CACHE_TABLES))).fetchone()
            if row is None or row[0] != len(SOURCE_CACHE_TABLES):
                return False

            existing = set(
                self._con.execute(
                    "SELECT table_name FROM information_schema.tables "
                    "WHERE table_schema = 'main'"
                ).fetchdf()["table_name"].tolist()
            )
            missing = [t for t in SOURCE_CACHE_TABLES if t not in existing]
            if missing:
                log.warning(
                    "[table_cache] Metadata says fresh but tables missing: %s — re-downloading",
                    missing,
                )
                return False
            return True
        except Exception:
            return False

    @staticmethod
    def _sanitize(df: pd.DataFrame) -> pd.DataFrame:
        """
        Replace MySQL zero dates, coerce datetime columns to datetime64,
        and convert Decimal columns to float64 for DuckDB compatibility.
        """
        import datetime as _dt
        from decimal import Decimal as _Decimal

        _ZERO_STR = "0000-00-00 00:00:00"
        _ZERO_DT  = _dt.datetime(1, 1, 1, 0, 0)

        for col in df.columns:
            if df[col].dtype == object:
                df[col] = df[col].replace(_ZERO_STR, None)
                non_null = df[col].dropna()
                if non_null.empty:
                    continue
                sample = non_null.iloc[0]
                if isinstance(sample, _dt.datetime):
                    df[col] = pd.to_datetime(df[col], errors="coerce")
                elif isinstance(sample, _Decimal):
                    df[col] = pd.to_numeric(df[col], errors="coerce")
            elif pd.api.types.is_datetime64_any_dtype(df[col]):
                pass
            else:
                try:
                    mask = df[col].apply(
                        lambda v: isinstance(v, _dt.datetime) and v == _ZERO_DT
                    )
                    if mask.any():
                        df[col] = pd.to_datetime(
                            df[col].where(~mask, pd.NaT), errors="coerce"
                        )
                except Exception:
                    pass
        return df

    @staticmethod
    def _fetch_with_retry(sql: str, params, max_retries: int = 3, base_wait: float = 5.0) -> pd.DataFrame:
        """Wrap db.fetch() with exponential-backoff retry for lost-connection errors."""
        last_exc = None
        for attempt in range(1, max_retries + 2):
            try:
                return fetch(SOURCE_DB, sql, params)
            except Exception as exc:
                last_exc = exc
                err_msg  = str(exc).lower()
                is_conn  = any(k in err_msg for k in (
                    "lost connection", "server has gone away",
                    "broken pipe", "connection reset", "timed out",
                    "can't connect", "connection refused",
                ))
                if not is_conn or attempt > max_retries:
                    raise
                wait = base_wait * (2 ** (attempt - 1))
                log.warning(
                    "[table_cache] Lost connection (attempt %d/%d) — retrying in %.0fs  (%s)",
                    attempt, max_retries, wait, exc,
                )
                time.sleep(wait)
        raise last_exc

    def refresh(self):
        """Fetch all source tables from production and store in DuckDB."""
        col_w = max(len(t) for t in SOURCE_CACHE_TABLES) + 2
        log.info("[table_cache] ── Caching source tables from production ──────────────")
        log.info("[table_cache]   %-*s  %10s  %s", col_w, "table", "rows", "status")
        log.info("[table_cache]   %s", "─" * 55)

        for table in SOURCE_CACHE_TABLES:
            try:
                df = self._sanitize(self._fetch_with_retry(f"SELECT * FROM {table}", None))
                self._con.execute(f"DROP TABLE IF EXISTS {table}")
                self._con.register("_tmp", df)
                self._con.execute(f"CREATE TABLE {table} AS SELECT * FROM _tmp")
                self._con.unregister("_tmp")
                now = datetime.now(timezone.utc).replace(tzinfo=None)
                self._con.execute("""
                    INSERT INTO source_table_meta (table_name, row_count, cached_at)
                    VALUES (?, ?, ?)
                    ON CONFLICT (table_name) DO UPDATE SET
                        row_count = excluded.row_count,
                        cached_at = excluded.cached_at
                """, [table, len(df), now])
                log.info("[table_cache]   %-*s  %10d  ✓ cached", col_w, table, len(df))
            except Exception as exc:
                log.error("[table_cache]   %-*s             ✗ FAILED: %s", col_w, table, exc)
                raise

        log.info("[table_cache] ─────────────────────────────────────────────────────")
        log.info("[table_cache] All source tables cached")

    def log_status(self):
        """Log current cache status: table, row count, when cached."""
        try:
            df    = self._con.execute(
                "SELECT table_name, row_count, cached_at FROM source_table_meta ORDER BY table_name"
            ).fetchdf()
            col_w = max(len(t) for t in SOURCE_CACHE_TABLES) + 2
            log.info("[table_cache] ── Cache status ──────────────────────────────────────")
            if df.empty:
                log.info("[table_cache]   No tables cached yet")
            else:
                log.info("[table_cache]   %-*s  %10s  %s", col_w, "table", "rows", "cached at")
                log.info("[table_cache]   %s", "─" * 60)
                for _, row in df.iterrows():
                    log.info(
                        "[table_cache]   %-*s  %10d  %s",
                        col_w, row["table_name"], row["row_count"], row["cached_at"],
                    )
            log.info("[table_cache] ─────────────────────────────────────────────────────")
        except Exception as exc:
            log.warning("[table_cache] Could not read cache status: %s", exc)

    # ── Completion table SQL ──────────────────────────────────────────────────

    _LA_BATCH_SQL = """
        SELECT la.*
        FROM learning_activities la
        WHERE la.completed = 1
          AND la.user_id IN ({placeholders})
    """

    _FLA_BATCH_SQL = """
        SELECT fla.*
        FROM facilitator_learning_activities fla
        WHERE fla.completed = 1
          AND fla.user_id IN ({placeholders})
    """

    _LA_INCR_SQL = """
        SELECT la.*
        FROM learning_activities la
        WHERE la.completed    = 1
          AND la.completed_at > %s
          AND la.user_id IN (
              SELECT id FROM users
              WHERE status = 1 AND deleted_at IS NULL AND type IN (3, 4)
          )
    """

    _FLA_INCR_SQL = """
        SELECT fla.*
        FROM facilitator_learning_activities fla
        WHERE fla.completed    = 1
          AND fla.completed_at > %s
          AND fla.user_id IN (
              SELECT id FROM users
              WHERE status = 1 AND deleted_at IS NULL AND type IN (1, 2)
          )
    """

    _LESSON_FILTER_SQL = """
        DELETE FROM {table}
        WHERE lesson_id NOT IN (
            SELECT DISTINCT l.id
            FROM   centre_subject cs
            LEFT JOIN lessons  l ON l.subject_id = cs.subject_id
            LEFT JOIN subjects s ON s.id          = cs.subject_id
            WHERE  l.id IS NOT NULL
              AND  s.status     = 1 AND s.deleted_at IS NULL
              AND  l.status     = 1 AND l.deleted_at IS NULL
        )
    """

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _dedup_completion_table(self, table: str):
        before = self._con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        self._con.execute(f"""
            CREATE TABLE _dedup AS
            SELECT * EXCLUDE (_rn)
            FROM (
                SELECT *,
                       ROW_NUMBER() OVER (
                           PARTITION BY user_id, lesson_id
                           ORDER BY completed_at DESC NULLS LAST
                       ) AS _rn
                FROM {table}
            )
            WHERE _rn = 1
        """)
        self._con.execute(f"DROP TABLE {table}")
        self._con.execute(f"ALTER TABLE _dedup RENAME TO {table}")
        after   = self._con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        removed = before - after
        log.info(
            "[table_cache]   %-35s  dedup: %d → %d rows (%d duplicate attempts removed)",
            table, before, after, removed,
        )

    def _get_last_completion_ts(self, table: str) -> Optional[str]:
        try:
            row = self._con.execute(
                "SELECT str_value FROM cache_meta WHERE key = ?",
                [f"{table}_max_completed_at"],
            ).fetchone()
            return row[0] if row and row[0] else None
        except Exception:
            return None

    def _save_last_completion_ts(self, table: str, ts: str):
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        self._con.execute(
            """
            INSERT INTO cache_meta (key, int_value, str_value, updated_at)
            VALUES (?, NULL, ?, ?)
            ON CONFLICT (key) DO UPDATE SET
                str_value  = excluded.str_value,
                updated_at = excluded.updated_at
            """,
            [f"{table}_max_completed_at", str(ts), now],
        )

    def _upsert_source_meta(self, table: str, row_count: int):
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        self._con.execute(
            """
            INSERT INTO source_table_meta (table_name, row_count, cached_at)
            VALUES (?, ?, ?)
            ON CONFLICT (table_name) DO UPDATE SET
                row_count = excluded.row_count,
                cached_at = excluded.cached_at
            """,
            [table, row_count, now],
        )

    def _apply_lesson_filter_in_duckdb(self, table: str):
        before  = self._con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        self._con.execute(self._LESSON_FILTER_SQL.format(table=table))
        after   = self._con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        removed = before - after
        log.info(
            "[table_cache]   %-35s  lesson filter: removed %d inactive rows (%d → %d)",
            table, removed, before, after,
        )

    # ── Public refresh ────────────────────────────────────────────────────────

    def refresh_completion_tables(self, batch_size: int = 2000, incremental: bool = True):
        """
        Cache learning_activities and facilitator_learning_activities in DuckDB.

        Incremental mode (default): appends only rows newer than last cached ts.
        Full mode (--force-refresh): drops and rebuilds from scratch in batches.
        """
        col_w = len("facilitator_learning_activities") + 2
        specs = [
            ("learning_activities",             "3, 4",
             self._LA_BATCH_SQL, self._LA_INCR_SQL),
            ("facilitator_learning_activities", "1, 2",
             self._FLA_BATCH_SQL, self._FLA_INCR_SQL),
        ]

        for table, types_sql, batch_sql, incr_sql in specs:
            try:
                last_ts = self._get_last_completion_ts(table) if incremental else None

                # ── Incremental path ──────────────────────────────────────────
                if last_ts:
                    log.info("[table_cache] ── %s: incremental refresh ─────────────────", table)
                    log.info("[table_cache]   last cached completed_at: %s", last_ts)

                    df = self._sanitize(self._fetch_with_retry(incr_sql, (last_ts,)))

                    if df.empty:
                        log.info(
                            "[table_cache]   %-*s  no new records since %s — cache up to date",
                            col_w, table, last_ts,
                        )
                        log.info("[table_cache] ─────────────────────────────────────────────────────")
                        continue

                    before_dedup = len(df)
                    df = (
                        df.sort_values("completed_at", ascending=False, na_position="last")
                          .drop_duplicates(subset=["user_id", "lesson_id"], keep="first")
                          .reset_index(drop=True)
                    )
                    if len(df) < before_dedup:
                        log.info(
                            "[table_cache]   %-*s  %d duplicate attempts in new batch removed",
                            col_w, table, before_dedup - len(df),
                        )

                    self._con.register("_incr_pairs", df[["user_id", "lesson_id"]])
                    self._con.execute(f"""
                        DELETE FROM {table}
                        WHERE (user_id, lesson_id) IN (
                            SELECT user_id, lesson_id FROM _incr_pairs
                        )
                    """)
                    self._con.unregister("_incr_pairs")

                    self._con.register("_incr", df)
                    self._con.execute(f"INSERT INTO {table} SELECT * FROM _incr")
                    self._con.unregister("_incr")

                    new_ts = str(df["completed_at"].max())
                    self._save_last_completion_ts(table, new_ts)

                    total = self._con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                    self._upsert_source_meta(table, total)

                    log.info(
                        "[table_cache]   %-*s  upserted %d records  (total: %d rows)  new max_ts: %s",
                        col_w, table, len(df), total, new_ts,
                    )
                    log.info("[table_cache] ─────────────────────────────────────────────────────")
                    continue

                # ── Full refresh path ─────────────────────────────────────────
                log.info("[table_cache] ── %s: full refresh (batch_size=%d) ──────────", table, batch_size)

                user_ids = self._con.execute(f"""
                    SELECT id FROM users
                    WHERE status = 1 AND deleted_at IS NULL
                      AND type IN ({types_sql})
                """).fetchdf()["id"].tolist()

                n_users   = len(user_ids)
                chunks    = [user_ids[i:i + batch_size] for i in range(0, n_users, batch_size)]
                n_chunks  = len(chunks)

                log.info(
                    "[table_cache]   %-*s  %d users → %d batches of %d",
                    col_w, table, n_users, n_chunks, batch_size,
                )

                self._con.execute(f"DROP TABLE IF EXISTS {table}")
                table_created = False
                total_rows    = 0
                max_ts        = None

                for i, chunk in enumerate(chunks, 1):
                    placeholders = ", ".join(["%s"] * len(chunk))
                    sql = batch_sql.format(placeholders=placeholders)
                    df  = self._sanitize(self._fetch_with_retry(sql, tuple(chunk)))

                    if not df.empty:
                        self._con.register("_tmp", df)
                        if not table_created:
                            self._con.execute(f"CREATE TABLE {table} AS SELECT * FROM _tmp")
                            table_created = True
                        else:
                            self._con.execute(f"INSERT INTO {table} SELECT * FROM _tmp")
                        self._con.unregister("_tmp")
                        total_rows += len(df)

                        if "completed_at" in df.columns:
                            batch_max = df["completed_at"].max()
                            if pd.notna(batch_max):
                                if max_ts is None or batch_max > max_ts:
                                    max_ts = batch_max

                    if i % 50 == 0 or i == n_chunks:
                        log.info(
                            "[table_cache]   %-*s  batch %d / %d  (rows so far: %d)",
                            col_w, table, i, n_chunks, total_rows,
                        )

                if not table_created:
                    self._con.execute(
                        f"CREATE TABLE IF NOT EXISTS {table} "
                        f"(user_id VARCHAR, lesson_id VARCHAR, score DOUBLE, "
                        f"rating DOUBLE, duration BIGINT, completed INTEGER)"
                    )

                if table_created:
                    self._apply_lesson_filter_in_duckdb(table)
                    self._dedup_completion_table(table)
                    total_rows = self._con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]

                self._upsert_source_meta(table, total_rows)

                if max_ts is not None:
                    self._save_last_completion_ts(table, str(max_ts))
                    log.info("[table_cache]   %-*s  max completed_at saved: %s", col_w, table, max_ts)

                log.info("[table_cache]   %-*s  %10d total rows  ✓ cached", col_w, table, total_rows)
                log.info("[table_cache] ─────────────────────────────────────────────────────")

            except Exception as exc:
                log.error("[table_cache]   ✗ FAILED %s: %s", table, exc)
                raise

        log.info("[table_cache] Completion tables ready — s3 will query DuckDB this run")

    # ── DuckDB indexes ────────────────────────────────────────────────────────

    def build_indexes(self):
        """
        Create ART indexes on the columns most frequently used as JOIN keys
        in the allocation queries.  Reduces per-chunk JOIN time from ~38s to ~3-5s.
        """
        _index_specs = [
            ("batch_subject",        "batch_id"),
            ("batch_subject",        "subject_id"),
            ("centre_subject",       "centre_id"),
            ("centre_subject",       "subject_id"),
            ("student_details",      "user_id"),
            ("ple_career_path_user", "user_id"),
            ("learning_activities",             "user_id"),
            ("facilitator_learning_activities", "user_id"),
        ]
        built = 0
        for table, col in _index_specs:
            idx_name = f"idx_{table}_{col}"
            try:
                self._con.execute(
                    f"CREATE INDEX IF NOT EXISTS {idx_name} ON {table}({col})"
                )
                built += 1
            except Exception as exc:
                log.warning("[table_cache] Could not create index %s: %s", idx_name, exc)
        log.info("[table_cache] Built %d DuckDB indexes", built)

    # ── Two-stage precompute: user × subject map ──────────────────────────────

    def precompute_user_subject_map(self, learner_types_sql: str) -> int:
        """
        Stage 1 of two-stage precompute.

        Runs the three allocation queries WITHOUT the lessons JOIN, producing a
        _user_subject_map table (~21 M rows) instead of the full user × lesson
        table (~370 M rows).  Takes ~5 minutes instead of >1 hour.

        Stage 2 happens per-chunk in load_alloc_for_chunk(): a fast indexed
        DuckDB JOIN of _user_subject_map with lessons for ~2 000 users.
        """
        from steps.s2_allocation import (
            _NON_PLE_SQL, _PLE_SQL,
            _SUBJECT_SELECT, _SUBJECT_JOINS,
            _STAFF_SUBJECT_SQL,
        )

        log.info("[table_cache] ── Building user×subject map (stage-1 precompute) ──────")
        t0 = time.time()

        # Live progress bar in the terminal for each long-running CREATE TABLE,
        # so a slow build is visibly progressing rather than looking stuck.
        try:
            self._con.execute("SET enable_progress_bar = true")
        except Exception:
            pass

        def _build_learner(template, path_name, basis):
            # Build the inner SQL using _SUBJECT_SELECT directly — no string
            # literal injection inside the inner query.  allocation_path and
            # allocation_basis are added by the outer CREATE TABLE wrapper,
            # avoiding the DuckDB parser error caused by injecting 'non_ple'
            # inside a nested SELECT via f-string concatenation.
            extra = "\n    NULL                        AS is_master_trainer,"
            sql = template.format(
                common_select=extra + _SUBJECT_SELECT,
                lesson_joins=_SUBJECT_JOINS,
                types=learner_types_sql,
                user_clause="",
            )
            sql = self._adapt_sql_for_duckdb(sql)
            return self._strip_trailing_order_by(sql), path_name, basis

        non_ple_sql, non_ple_path, non_ple_basis = _build_learner(
            _NON_PLE_SQL, "non_ple",
            "centre_subject [-> batch_subject if batch] [-> subject_trade if trade]",
        )
        ple_sql, ple_path, ple_basis = _build_learner(
            _PLE_SQL, "ple",
            "centre_subject [-> subject_ple_career_path if career_path] [-> batch_subject if batch]",
        )
        raw_staff_sql = self._adapt_sql_for_duckdb(
            self._strip_trailing_order_by(
                _STAFF_SUBJECT_SQL.format(user_clause="")
            )
        )

        for tbl in ("_usm_non_ple", "_usm_ple", "_usm_staff", "_user_subject_map"):
            self._con.execute(f"DROP TABLE IF EXISTS {tbl}")

        path_specs = [
            ("_usm_non_ple", non_ple_sql, non_ple_path, non_ple_basis),
            ("_usm_ple",     ple_sql,     ple_path,     ple_basis),
            # staff SQL already contains allocation_path/allocation_basis literals
            # (they're baked into _STAFF_SUBJECT_SQL), so we pass empty strings
            # and skip the outer injection for staff.
            ("_usm_staff",   raw_staff_sql, None,        None),
        ]
        for tbl, sql, path_name, basis in path_specs:
            log.info("[table_cache]   %s: building subject map ...", path_name or "staff")
            t1 = time.time()
            if path_name is not None:
                # Pass allocation_path and allocation_basis as ? parameters so
                # the string literals are never embedded inside the SQL string.
                # This is what caused the original DuckDB ParserException:
                #   syntax error at or near "'non_ple'"
                # — the literal was injected raw inside a nested SELECT via
                # f-string, which DuckDB couldn't parse in that context.
                wrapper = (
                    f"CREATE TABLE {tbl} AS "
                    f"SELECT * EXCLUDE (career_path_updated_at), "
                    f"       TRY_CAST(career_path_updated_at AS TIMESTAMP) AS career_path_updated_at, "
                    f"       TRY_CAST(is_master_trainer AS INTEGER)        AS is_master_trainer, "
                    f"       ? AS allocation_path, "
                    f"       ? AS allocation_basis "
                    f"FROM ({sql}) _q"
                )
                self._con.execute(wrapper, [path_name, basis])
            else:
                # staff SQL already carries allocation_path/allocation_basis
                wrapper = (
                    f"CREATE TABLE {tbl} AS "
                    f"SELECT * EXCLUDE (career_path_updated_at), "
                    f"       TRY_CAST(career_path_updated_at AS TIMESTAMP) AS career_path_updated_at, "
                    f"       TRY_CAST(is_master_trainer AS INTEGER)        AS is_master_trainer "
                    f"FROM ({sql}) _q"
                )
                self._con.execute(wrapper)
            cnt = self._con.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
            log.info("[table_cache]   %s: %d rows  (%.0fs)", path_name or "staff", cnt, time.time() - t1)

        log.info("[table_cache]   combining paths → _user_subject_map ...")
        self._con.execute("""
            CREATE TABLE _user_subject_map AS
            SELECT * FROM _usm_non_ple
            UNION ALL
            SELECT * FROM _usm_ple
            UNION ALL
            SELECT * FROM _usm_staff
        """)
        for tbl in ("_usm_non_ple", "_usm_ple", "_usm_staff"):
            self._con.execute(f"DROP TABLE IF EXISTS {tbl}")

        self._con.execute(
            "CREATE INDEX IF NOT EXISTS idx_usm_user_id ON _user_subject_map(user_id)"
        )
        total = self._con.execute("SELECT COUNT(*) FROM _user_subject_map").fetchone()[0]
        log.info(
            "[table_cache] _user_subject_map ready — %d rows  (%.0fs total)",
            total, time.time() - t0,
        )
        return total

    def user_subject_map_exists(self) -> bool:
        """True if _user_subject_map is present in DuckDB."""
        try:
            return self._con.execute(
                "SELECT COUNT(*) FROM information_schema.tables "
                "WHERE table_name = '_user_subject_map'"
            ).fetchone()[0] > 0
        except Exception:
            return False

    def load_alloc_for_chunk(self, chunk_ids: list, chunk_type: str) -> pd.DataFrame:
        """
        Stage 2 of two-stage precompute.

        Looks up the chunk's users in _user_subject_map (indexed scan),
        then JOINs with lessons / lesson_types in one DuckDB query.
        Returns a DataFrame with the same columns as fetch_allocation().
        """
        if not chunk_ids:
            return pd.DataFrame()

        ph = ", ".join(["?" for _ in chunk_ids])

        if chunk_type == "staff":
            path_filter = "usm.allocation_path = 'staff'"
            lesson_access = """
              AND (
                  usm.user_type = 1
                  OR (usm.user_type = 2
                      AND (usm.is_master_trainer IS NULL OR usm.is_master_trainer != 1)
                      AND l.facilitator_access = 1)
                  OR (usm.user_type = 2
                      AND usm.is_master_trainer = 1
                      AND l.mastertrainer_access = 1)
              )"""
            student_filter = ""
        else:
            path_filter = "usm.allocation_path IN ('non_ple', 'ple')"
            lesson_access = ""
            student_filter = "AND l.student_access = 1"

        sql = f"""
            SELECT
                usm.user_id,
                usm.user_name,
                usm.user_type,
                usm.centre_id,
                usm.project_id,
                usm.batch_id,
                usm.trade_id,
                usm.career_path_id,
                usm.career_path_name,
                usm.career_path_updated_at,
                usm.subject_id,
                usm.subject_name,
                usm.subject_is_ple,
                usm.ple_career_path_id,
                usm.year_to_map,
                usm.subject_order,
                l.id            AS lesson_id,
                l.name          AS lesson_name,
                l.lesson_order,
                lt.name         AS lesson_type,
                CASE WHEN l.is_assessment = 1
                          OR upper(l.name) LIKE '%ASSESSMENT%'
                     THEN 1 ELSE 0
                END             AS is_assessment,
                CASE
                    WHEN l.student_access       = 1 THEN 'student'
                    WHEN l.facilitator_access   = 1 THEN 'facilitator'
                    WHEN l.mastertrainer_access = 1 THEN 'master'
                    ELSE NULL
                END             AS toolkit_type,
                usm.trade_duration,
                usm.allocation_path,
                usm.allocation_basis
            FROM _user_subject_map usm
            JOIN lessons l
                ON  l.subject_id         = usm.subject_id
                AND l.status             = 1
                AND l.deleted_at         IS NULL
                AND l.lesson_category_id = 'd78bc322-568f-4110-8e24-02ea444d48b7'
                {student_filter}
            LEFT JOIN lesson_types lt ON lt.id = l.lesson_type_id
            WHERE usm.user_id IN ({ph})
              AND {path_filter}
              {lesson_access}
        """
        # Cursor per call — read concurrently by CHUNK_WORKERS threads.
        cur = self._con.cursor()
        try:
            df = cur.execute(sql, chunk_ids).fetchdf()
        finally:
            cur.close()
        log.debug("[table_cache] load_alloc_for_chunk → %d rows (%s)", len(df), chunk_type)
        return df

    # ── Opt 1 — Precompute full allocation ────────────────────────────────────

    _ORDER_BY_RE = re.compile(r'\s+ORDER\s+BY\b.*', re.IGNORECASE | re.DOTALL)

    @staticmethod
    def _strip_trailing_order_by(sql: str) -> str:
        """Remove only the top-level trailing ORDER BY (not ones inside subqueries)."""
        import re
        # Walk backwards through ORDER BY occurrences and remove the last one
        # whose position is not inside a subquery (paren depth == 0).
        pattern = re.compile(r'\bORDER\s+BY\b', re.IGNORECASE)
        matches = list(pattern.finditer(sql))
        for m in reversed(matches):
            depth = sql[:m.start()].count('(') - sql[:m.start()].count(')')
            if depth == 0:
                return sql[:m.start()].rstrip()
        return sql

    @staticmethod
    def _adapt_sql_for_duckdb(sql: str) -> str:
        return sql.replace("`", '"').replace("%s", "?")

    def precompute_allocation(self, learner_types_sql: str) -> int:
        """
        Run the full allocation query for ALL users in one DuckDB pass and
        store the combined deduplicated result as _alloc_precomputed.

        Instead of running 2 JOINs × 469 chunks = 938 queries, this runs
        3 CREATE TABLE AS SELECT statements (one per path) and one UNION ALL +
        dedup pass.  Each chunk then reads via a sub-second indexed scan.

        Expected time: ~15–30 min vs ~10 hours for the per-chunk approach.
        Returns total row count of the pre-computed table.
        """
        from steps.s2_allocation import (
            _NON_PLE_SQL, _PLE_SQL, _STAFF_SQL,
            _COMMON_SELECT, _COMMON_SELECT_STAFF,
            _LESSON_JOINS, _LESSON_JOINS_STAFF,
        )

        log.info("[table_cache] ── Pre-computing full allocation (all users, 1 pass) ──────")
        t0 = time.time()

        def _build(template, common_select, lesson_joins, types=""):
            sql = template.format(
                common_select=common_select,
                lesson_joins=lesson_joins,
                types=types,
                user_clause="",
            )
            sql = self._adapt_sql_for_duckdb(sql)
            sql = self._strip_trailing_order_by(sql)
            return sql

        non_ple_sql = _build(_NON_PLE_SQL, _COMMON_SELECT,      _LESSON_JOINS,       types=learner_types_sql)
        ple_sql     = _build(_PLE_SQL,     _COMMON_SELECT,       _LESSON_JOINS,       types=learner_types_sql)
        staff_sql   = _build(_STAFF_SQL,   _COMMON_SELECT_STAFF, _LESSON_JOINS_STAFF)

        for _t in ("_alloc_non_ple", "_alloc_ple", "_alloc_staff", "_alloc_precomputed"):
            self._con.execute(f"DROP TABLE IF EXISTS {_t}")

        _path_specs = [
            ("_alloc_non_ple", non_ple_sql, "non_ple",
             "centre_subject [-> batch_subject if batch] [-> subject_trade if trade]"),
            ("_alloc_ple",     ple_sql,     "ple",
             "centre_subject [-> subject_ple_career_path if career_path] [-> batch_subject if batch]"),
            ("_alloc_staff",   staff_sql,   "staff",
             "centre_subject (admin: all; facilitator: facilitator_access; master_trainer: mastertrainer_access)"),
        ]

        for tbl, inner_sql, path_name, basis in _path_specs:
            log.info("[table_cache]   %s: running full join ...", path_name)
            t1 = time.time()
            self._con.execute(
                "CREATE TABLE " + tbl + " AS "
                "SELECT * EXCLUDE (career_path_updated_at), "
                "       TRY_CAST(career_path_updated_at AS TIMESTAMP) AS career_path_updated_at, "
                "       '" + path_name + "' AS allocation_path, "
                "       '" + basis + "' AS allocation_basis "
                "FROM (" + inner_sql + ") AS _q"
            )
            cnt = self._con.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
            log.info("[table_cache]   %s: %d rows  (%.0fs)", path_name, cnt, time.time() - t1)

        log.info("[table_cache]   combining + deduplicating all paths ...")
        t1 = time.time()
        self._con.execute("""
            CREATE TABLE _alloc_precomputed AS
            SELECT * EXCLUDE (career_path_updated_at, _rn)
            FROM (
                SELECT *,
                       ROW_NUMBER() OVER (
                           PARTITION BY user_id, lesson_id
                           ORDER BY career_path_updated_at DESC NULLS LAST
                       ) AS _rn
                FROM (
                    SELECT * FROM _alloc_non_ple
                    UNION ALL
                    SELECT * FROM _alloc_ple
                    UNION ALL
                    SELECT * FROM _alloc_staff
                )
            )
            WHERE _rn = 1
        """)

        for _t in ("_alloc_non_ple", "_alloc_ple", "_alloc_staff"):
            self._con.execute(f"DROP TABLE IF EXISTS {_t}")

        # Index for fast chunk lookups
        self._con.execute(
            "CREATE INDEX IF NOT EXISTS idx_alloc_precomputed_user_id "
            "ON _alloc_precomputed(user_id)"
        )

        total = self._con.execute("SELECT COUNT(*) FROM _alloc_precomputed").fetchone()[0]
        log.info(
            "[table_cache] ✓ _alloc_precomputed ready — %d rows  (total: %.0fs)",
            total, time.time() - t0,
        )
        log.info("[table_cache] ─────────────────────────────────────────────────────")
        return total

    def alloc_precomputed_exists(self) -> bool:
        """True if _alloc_precomputed is present in DuckDB."""
        try:
            return self._con.execute(
                "SELECT COUNT(*) FROM information_schema.tables "
                "WHERE table_name = '_alloc_precomputed'"
            ).fetchone()[0] > 0
        except Exception:
            return False

    def load_alloc_precomputed_chunk(self, user_ids: list) -> pd.DataFrame:
        """Return allocation rows for user_ids from the pre-computed table (indexed scan)."""
        if not user_ids:
            return pd.DataFrame()
        ph = ", ".join(["?" for _ in user_ids])
        return self._con.execute(
            f"SELECT * FROM _alloc_precomputed WHERE user_id IN ({ph})",
            user_ids,
        ).fetchdf()

    def drop_alloc_precomputed(self):
        self._con.execute("DROP TABLE IF EXISTS _alloc_precomputed")
        log.info("[table_cache] _alloc_precomputed dropped (cleanup)")

    # ── Opt 4 — Batch all-user completion fetch ───────────────────────────────

    def fetch_all_completion(self) -> pd.DataFrame:
        """
        Fetch ALL completion data from both cached tables in one DuckDB query.

        Returns a single DataFrame with columns:
            user_id, lesson_id, score, rating, data_from, duration

        Deduplicates to one row per (user_id, lesson_id) keeping the
        highest score.  main.py merges this once against the full allocation
        instead of fetching per chunk — eliminates ~635 SSH completion queries.
        """
        log.info("[table_cache] ── Batch completion fetch (all users, DuckDB) ────────")
        t0 = time.time()

        # Verify both tables exist before querying
        existing = set(
            self._con.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
            ).fetchdf()["table_name"].tolist()
        )

        frames = []
        for tbl in ("learning_activities", "facilitator_learning_activities"):
            if tbl not in existing:
                log.warning("[table_cache] %s not in DuckDB cache — skipping", tbl)
                continue
            df = self._con.execute(f"""
                SELECT
                    user_id,
                    lesson_id,
                    MAX(score)    AS score,
                    MAX(rating)   AS rating,
                    NULL          AS data_from,
                    SUM(duration) AS duration
                FROM {tbl}
                WHERE completed = 1
                GROUP BY user_id, lesson_id
            """).fetchdf()
            frames.append(df)
            log.info("[table_cache]   %s → %d rows", tbl, len(df))

        if not frames:
            log.warning("[table_cache] No completion tables found — returning empty")
            return pd.DataFrame(columns=["user_id", "lesson_id", "score", "rating", "data_from", "duration"])

        combined = pd.concat(frames, ignore_index=True)

        # Deduplicate across both tables (a user_id should only appear in one,
        # but guard against edge cases)
        combined = (
            combined
            .sort_values("score", ascending=False, na_position="last")
            .drop_duplicates(subset=["user_id", "lesson_id"], keep="first")
            .reset_index(drop=True)
        )

        log.info(
            "[table_cache] ✓ batch completion: %d rows  (%.0fs)",
            len(combined), time.time() - t0,
        )
        log.info("[table_cache] ─────────────────────────────────────────────────────")
        return combined

    # ── make_fetch_fn ─────────────────────────────────────────────────────────

    def make_fetch_fn(self):
        """
        Return a drop-in replacement for db.fetch() that queries DuckDB locally.
        Adapts MySQL SQL syntax (%s → ?, backtick → double-quote) automatically.
        """
        con = self._con

        def _duckdb_fetch(cfg, sql: str, params):
            adapted = sql.replace("`", '"').replace("%s", "?")
            # One cursor per call — DuckDB connections cannot run concurrent
            # queries, and this fetch_fn is shared by CHUNK_WORKERS threads.
            cur = con.cursor()
            try:
                return cur.execute(adapted, list(params) if params else []).fetchdf()
            finally:
                cur.close()

        return _duckdb_fetch


# ─────────────────────────────────────────────────────────────────────────────
# Opt 5 — Result Buffer  (streams chunk results to analytics DB at end of run)
# ─────────────────────────────────────────────────────────────────────────────

class ResultBuffer:
    """
    Accumulates chunk results in DuckDB during the loop, then streams
    everything to the analytics DB in one bulk write at the end.

    Why: each per-chunk write opens 2 SSH tunnels (one per table).
    For 635 chunks that is ~1,270 SSH handshakes just for writes.
    Buffering in DuckDB + one flush at the end reduces that to
    a single persistent connection (via TunnelPool).

    Active for full unscoped runs only (start_chunk=1, no --since).
    Falls back to per-chunk writes for --start-chunk resume and --since.
    """

    _TABLES = {
        "lesson":      "_rbuf_lesson",
        "subject":     "_rbuf_subject",
        "subject_all": "_rbuf_subject_all",
    }

    def __init__(self, con: duckdb.DuckDBPyConnection):
        self._con     = con
        self._created: set  = set()
        self._columns: dict = {}

    def append(self, key: str, df: pd.DataFrame):
        """Append one chunk's result to the DuckDB buffer table."""
        if df.empty:
            return
        buf = self._TABLES[key]
        if buf not in self._created:
            self._con.register("_rbuf", _ensure_varchar_nulls(df))
            self._con.execute(f"DROP TABLE IF EXISTS {buf}")
            self._con.execute(f"CREATE TABLE {buf} AS SELECT * FROM _rbuf")
            self._created.add(buf)
            self._columns[buf] = list(df.columns)
        else:
            schema_cols = self._columns[buf]
            if list(df.columns) != schema_cols:
                df = df.reindex(columns=schema_cols)
            self._con.register("_rbuf", df)
            self._con.execute(f"INSERT INTO {buf} SELECT * FROM _rbuf")
        self._con.unregister("_rbuf")

    def row_count(self, key: str) -> int:
        buf = self._TABLES[key]
        if buf not in self._created:
            return 0
        return self._con.execute(f"SELECT COUNT(*) FROM {buf}").fetchone()[0]

    def flush(
        self,
        key:             str,
        analytics_cfg:   dict,
        analytics_table: str,
        if_exists:       str = "replace",
        stream_chunk:    int = 100_000,
    ) -> int:
        """
        Stream rows from DuckDB buffer → analytics DB table.

        Uses a single persistent pymysql connection (via TunnelPool if active)
        for the entire flush so we don't re-open the SSH tunnel per batch.
        """
        from db import TunnelPool, _connect_or_pool, write_table_with_conn

        buf = self._TABLES[key]
        if buf not in self._created:
            log.info("[result_buf] %s — nothing buffered, skipping", analytics_table)
            return 0

        total = self._con.execute(f"SELECT COUNT(*) FROM {buf}").fetchone()[0]
        log.info(
            "[result_buf] flushing %-45s → %s  (%d rows in %d-row batches)",
            buf, analytics_table, total, stream_chunk,
        )

        # Open one connection for the whole flush — avoids per-batch SSH overhead
        with _connect_or_pool(analytics_cfg) as conn:
            cursor   = self._con.execute(f"SELECT * FROM {buf}")
            columns  = [d[0] for d in cursor.description]
            db_name  = analytics_cfg["db"]["database"]
            first    = True
            written  = 0

            while True:
                rows = cursor.fetchmany(stream_chunk)
                if not rows:
                    break
                df   = pd.DataFrame(rows, columns=columns)
                mode = if_exists if first else "append"
                write_table_with_conn(conn, db_name, df, analytics_table, if_exists=mode)
                first    = False
                written += len(df)
                log.info("[result_buf]   %s  %d / %d rows written", analytics_table, written, total)

        self._con.execute(f"DROP TABLE IF EXISTS {buf}")
        self._created.discard(buf)
        log.info("[result_buf] ✓ %s complete — %d rows", analytics_table, written)
        return written

    def drop_all(self):
        """Drop all buffer tables (cleanup on error or cancellation)."""
        for buf in self._TABLES.values():
            self._con.execute(f"DROP TABLE IF EXISTS {buf}")
        self._created.clear()

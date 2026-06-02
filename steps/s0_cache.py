"""
Local DuckDB cache layer — individual source tables + allocation result cache.

Two-layer caching strategy
──────────────────────────
Layer 1 — TableCache  (new)
  Caches every source table that is NOT learning_activities or
  facilitator_learning_activities into cache.duckdb.  On subsequent runs,
  s1_users and s2_allocation run their SQL queries locally against DuckDB
  instead of opening SSH tunnels to production MySQL.  Only s3_completion
  still hits production (it needs live completion data by design).

  Tables cached:
    users, student_details, subjects, lessons, lesson_types, trades,
    centre_subject, batch_subject, subject_trade,
    ple_career_paths, subject_ple_career_path, ple_career_path_user

  Refreshed when:
    • No cache exists (first run)
    • Any allocation watch table row count changed (same signal as Layer 2)
    • --force-refresh flag passed

Layer 2 — AllocationCache  (existing, unchanged)
  After s2 runs (whether against DuckDB or MySQL), the full allocation
  result is appended chunk-by-chunk to allocation_cache in DuckDB.  On
  runs where allocation is confirmed unchanged, each chunk is loaded
  directly from allocation_cache — skipping even the DuckDB JOIN queries.

  Fallback: if either layer fails for any reason, the pipeline falls back
  to live production queries — no data is ever lost or corrupted.

Cache file: cache.duckdb  (pipeline root, gitignored)
Use --force-refresh to bypass both layers and rebuild from scratch.
"""

import logging
import os
from datetime import datetime, timezone

import duckdb
import pandas as pd

from config import SOURCE_DB
from db import fetch

log = logging.getLogger(__name__)

_PIPELINE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_CACHE_PATH = os.path.join(_PIPELINE_DIR, "cache.duckdb")

# Tables whose row counts are snapshotted to detect allocation changes.
# A change in any one of these triggers a full allocation re-fetch.
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


class AllocationCache:
    def __init__(self, path: str = DEFAULT_CACHE_PATH):
        self.path = path
        self._con = duckdb.connect(path)
        self._init_schema()

    def _init_schema(self):
        self._con.execute("""
            CREATE TABLE IF NOT EXISTS cache_meta (
                key        VARCHAR PRIMARY KEY,
                int_value  BIGINT,
                str_value  VARCHAR,
                updated_at TIMESTAMP
            )
        """)
        self._con.execute("""
            CREATE TABLE IF NOT EXISTS allocation_row_counts (
                table_name VARCHAR PRIMARY KEY,
                row_count  BIGINT,
                snapped_at TIMESTAMP
            )
        """)

    # ── Allocation change detection ───────────────────────────────────────────

    def allocation_changed(self) -> bool:
        """
        Return True if any watch table row count differs from the last snapshot,
        or if no snapshot exists yet. A single COUNT(*) per table over SSH —
        much cheaper than fetching the full allocation.

        Logs a clear per-table status line so you can see exactly which tables
        were checked, their current row counts, and whether each hit the cache
        or detected a change.
        """
        existing = self._con.execute(
            "SELECT table_name, row_count, snapped_at FROM allocation_row_counts"
        ).fetchdf()

        if existing.empty:
            log.info("[cache] ── Cache status: NO SNAPSHOT FOUND ──────────────────────")
            log.info("[cache] First run — allocation will be fetched live and cached")
            log.info("[cache] ─────────────────────────────────────────────────────────")
            return True

        snapshot    = dict(zip(existing["table_name"], existing["row_count"]))
        snapped_at  = existing.set_index("table_name")["snapped_at"].to_dict()
        changed     = False
        changed_tbl = None

        log.info("[cache] ── Cache status: checking allocation tables ──────────────")
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
                log.warning("[cache]   %-*s  ERROR     %s — treating as changed", col_w, table, exc)
                changed     = True
                changed_tbl = table

        if changed:
            log.info("[cache] ── Result: CHANGED (trigger: %s) — full allocation refresh ─", changed_tbl)
        else:
            log.info("[cache] ── Result: ALL CACHED  ✓ — allocation will load from DuckDB ─")
        log.info("[cache] ─────────────────────────────────────────────────────────")
        return changed

    def save_row_count_snapshot(self):
        """Snapshot current row counts of all watch tables."""
        now   = datetime.now(timezone.utc).replace(tzinfo=None)
        saved = 0
        for table in ALLOCATION_WATCH_TABLES:
            try:
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
        log.info("[cache] Row-count snapshot saved (%d / %d tables)", saved, len(ALLOCATION_WATCH_TABLES))

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

    def init_table(self, sample_df: pd.DataFrame):
        """Create allocation_cache with the correct schema (no rows) from sample_df."""
        self._con.register("_sample", sample_df.head(0))
        self._con.execute("CREATE TABLE allocation_cache AS SELECT * FROM _sample")
        self._con.unregister("_sample")
        log.info("[cache] allocation_cache table created")

    def append(self, df: pd.DataFrame):
        """Append one chunk of allocation data to the cache."""
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
        return self._con.execute(
            f"SELECT * FROM allocation_cache WHERE user_id IN ({placeholders})",
            user_ids,
        ).fetchdf()

    def close(self):
        self._con.close()


# ── Source tables to cache locally ───────────────────────────────────────────
# learning_activities and facilitator_learning_activities are intentionally
# excluded — they are always fetched live from production (s3_completion).
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

    Once cached, s1_users and s2_allocation run their existing SQL queries
    against DuckDB instead of MySQL — no SSH tunnel needed for those steps.
    make_fetch_fn() returns a drop-in replacement for db.fetch() that:
      • adapts %s → ? (placeholder style)
      • adapts `identifier` → "identifier" (quoting style)
      • runs the query locally against DuckDB
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
        """True if every source table has been cached at least once."""
        try:
            row = self._con.execute("""
                SELECT COUNT(*) AS n
                FROM   source_table_meta
                WHERE  table_name IN ({})
            """.format(", ".join(f"'{t}'" for t in SOURCE_CACHE_TABLES))).fetchone()
            return row is not None and row[0] == len(SOURCE_CACHE_TABLES)
        except Exception:
            return False

    def refresh(self):
        """Fetch all source tables from production and store in DuckDB."""
        col_w = max(len(t) for t in SOURCE_CACHE_TABLES) + 2
        log.info("[table_cache] ── Caching source tables from production ──────────────")
        log.info("[table_cache]   %-*s  %10s  %s", col_w, "table", "rows", "status")
        log.info("[table_cache]   %s", "─" * 55)

        for table in SOURCE_CACHE_TABLES:
            try:
                df = fetch(SOURCE_DB, f"SELECT * FROM {table}", None)
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
        log.info("[table_cache] All source tables cached — s1/s2 will run from DuckDB")

    def log_status(self):
        """Log current cache status: table, row count, when cached."""
        try:
            df = self._con.execute(
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

    def make_fetch_fn(self):
        """
        Return a fetch function with the same signature as db.fetch() that
        runs queries against the local DuckDB cache instead of MySQL.

        SQL adaptations applied automatically:
          %s  →  ?          (DuckDB placeholder style)
          `x` →  "x"        (DuckDB identifier quoting)
        """
        con = self._con

        def _duckdb_fetch(cfg, sql: str, params):
            adapted = sql.replace("`", '"').replace("%s", "?")
            return con.execute(adapted, list(params) if params else []).fetchdf()

        return _duckdb_fetch

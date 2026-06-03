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
import time
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

    def append(self, df: pd.DataFrame):
        """
        Append one chunk of allocation data to the cache.
        Creates the table on the first call (using real data for type inference).
        Empty DataFrame head(0) was causing DuckDB to infer UUID columns as INT32.
        """
        self._con.register("_chunk", df)
        exists = self._con.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'allocation_cache'"
        ).fetchone()[0]
        if not exists:
            self._con.execute("CREATE TABLE allocation_cache AS SELECT * FROM _chunk")
            log.info("[cache] allocation_cache table created from first chunk")
        else:
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
        """
        True only when every source table:
          1. Has a row in source_table_meta (metadata logged), AND
          2. Actually exists as a table in DuckDB (data is present).
        Checking both guards against partial/interrupted first runs where
        metadata was saved but data was never written (or the file was
        corrupted), which previously made is_fresh() return True incorrectly
        and caused the pipeline to query non-existent DuckDB tables.
        """
        try:
            # 1. Metadata check
            row = self._con.execute("""
                SELECT COUNT(*) AS n
                FROM   source_table_meta
                WHERE  table_name IN ({})
            """.format(", ".join(f"'{t}'" for t in SOURCE_CACHE_TABLES))).fetchone()
            if row is None or row[0] != len(SOURCE_CACHE_TABLES):
                return False

            # 2. Actual table existence check
            existing = set(
                self._con.execute(
                    "SELECT table_name FROM information_schema.tables "
                    "WHERE table_schema = 'main'"
                ).fetchdf()["table_name"].tolist()
            )
            missing = [t for t in SOURCE_CACHE_TABLES if t not in existing]
            if missing:
                log.warning(
                    "[table_cache] Metadata says fresh but tables missing in DuckDB: %s "
                    "— will re-download", missing
                )
                return False

            return True
        except Exception:
            return False

    @staticmethod
    def _sanitize(df: pd.DataFrame) -> pd.DataFrame:
        """
        Replace MySQL zero dates and convert datetime-like object columns to
        proper datetime64 dtype before loading into DuckDB.

        Two problems solved:
        1. DuckDB rejects MySQL zero dates (0000-00-00 00:00:00) with
           "timestamp field value out of range".
        2. After replacing zero dates with None, a column mixing
           datetime.datetime objects with float NaN causes pandas .max()
           to raise '>=' not supported between datetime.datetime and float.

        Fix: replace zero dates, then convert any object column that contains
        datetime objects to datetime64 via pd.to_datetime(errors='coerce').
        NaT becomes the uniform null sentinel — safe for DuckDB and pandas.
        """
        import datetime as _dt
        from decimal import Decimal as _Decimal

        _ZERO_STR = "0000-00-00 00:00:00"
        _ZERO_DT  = _dt.datetime(1, 1, 1, 0, 0)

        for col in df.columns:
            if df[col].dtype == object:
                # Replace zero-date strings first
                df[col] = df[col].replace(_ZERO_STR, None)

                non_null = df[col].dropna()
                if non_null.empty:
                    continue
                sample = non_null.iloc[0]

                if isinstance(sample, _dt.datetime):
                    # Datetime column — convert to datetime64.
                    # Keeps NaT as the null sentinel (safe for DuckDB and
                    # pandas .max()). Without this, None mixes with datetime
                    # objects causing '>=' not supported between datetime and float.
                    df[col] = pd.to_datetime(df[col], errors="coerce")

                elif isinstance(sample, _Decimal):
                    # MySQL DECIMAL columns — pymysql returns Python Decimal objects.
                    # DuckDB infers a narrow DECIMAL(p,s) from the first batch
                    # and rejects larger values in later batches (e.g. 100.0000
                    # overflows DECIMAL(6,4)). Convert to float64 so DuckDB uses
                    # DOUBLE, which has no precision overflow issues.
                    df[col] = pd.to_numeric(df[col], errors="coerce")

            elif pd.api.types.is_datetime64_any_dtype(df[col]):
                # Already proper dtype — NaT is safe for DuckDB
                pass

            else:
                # Edge case: non-object column holding datetime sentinel objects
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
        """
        Wrap db.fetch() with retry logic for lost-connection errors.

        MySQL drops long-running connections due to wait_timeout or net_read_timeout.
        Each batch in refresh_completion_tables() opens its own SSH tunnel, so a
        lost connection on one batch doesn't affect the others — we just retry.

        Retries: up to max_retries attempts with exponential backoff
          attempt 1 failure → wait 5s  → retry
          attempt 2 failure → wait 10s → retry
          attempt 3 failure → wait 20s → retry
          attempt 4 failure → raise
        """
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
                    "[table_cache] Lost connection on attempt %d/%d — retrying in %.0fs  (%s)",
                    attempt, max_retries, wait, exc,
                )
                time.sleep(wait)
        raise last_exc  # unreachable but satisfies linters

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

    # ── Full-refresh batch SQL (user_id chunks) ───────────────────────────────
    # Lesson-id filter removed from production queries — it ran as a subquery
    # on EVERY batch (468 times), causing each batch to take 3+ minutes.
    # The lesson filter is now applied ONCE in DuckDB after all batches load
    # (see _apply_lesson_filter_in_duckdb), using the already-cached lessons,
    # subjects, and centre_subject tables — fast local JOIN, zero SSH cost.
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

    # ── Incremental SQL (completed_at > last cached timestamp) ────────────────
    # Lesson filter also moved to post-load DuckDB step (same reason).
    # User filter kept here — limits result set to active users only.
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

    # ── DuckDB post-load lesson filter ────────────────────────────────────────
    # Runs once after all batches are loaded, using locally cached tables.
    # Removes any rows whose lesson_id is not an active lesson in centre_subject.
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

    def _get_last_completion_ts(self, table: str) -> str | None:
        """Return the stored MAX(completed_at) for a completion table, or None."""
        try:
            row = self._con.execute(
                "SELECT str_value FROM cache_meta WHERE key = ?",
                [f"{table}_max_completed_at"],
            ).fetchone()
            return row[0] if row and row[0] else None
        except Exception:
            return None

    def _save_last_completion_ts(self, table: str, ts: str):
        """Persist MAX(completed_at) after a full or incremental cache run."""
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
        """
        Remove rows whose lesson_id is not an active lesson in centre_subject.
        Runs once in DuckDB after all batches are loaded — uses already-cached
        lessons, subjects, centre_subject tables (local, zero SSH cost).
        """
        before = self._con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        self._con.execute(self._LESSON_FILTER_SQL.format(table=table))
        after  = self._con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        removed = before - after
        log.info(
            "[table_cache]   %-35s  lesson filter: removed %d inactive rows (%d → %d)",
            table, removed, before, after,
        )

    # ── Public refresh method ─────────────────────────────────────────────────

    def refresh_completion_tables(self, batch_size: int = 2000, incremental: bool = True):
        """
        Cache learning_activities and facilitator_learning_activities in DuckDB.

        Incremental mode (default, incremental=True):
          Checks whether a MAX(completed_at) snapshot exists for each table.
          If yes → fetches only rows WHERE completed_at > last_ts and appends.
          If no  → falls back to full batch refresh automatically.

          The s3_completion SQL already uses MAX(score)/SUM(duration) with
          GROUP BY (user_id, lesson_id), so appended duplicate keys are
          handled correctly by aggregation at query time.

        Full mode (incremental=False, used with --force-refresh):
          Drops and rebuilds each table from scratch in user_id batches.

        batch_size controls how many user_ids are fetched per production
        query during full refresh. Has no effect in incremental mode.
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
                    log.info(
                        "[table_cache] ── %s: incremental refresh ─────────────────",
                        table,
                    )
                    log.info("[table_cache]   last cached completed_at : %s", last_ts)
                    log.info("[table_cache]   fetching records newer than that ...")

                    df = self._sanitize(self._fetch_with_retry(incr_sql, (last_ts,)))

                    if df.empty:
                        log.info(
                            "[table_cache]   %-*s  no new records since %s — cache up to date",
                            col_w, table, last_ts,
                        )
                        log.info("[table_cache] ─" * 30)
                        continue

                    self._con.register("_incr", df)
                    self._con.execute(f"INSERT INTO {table} SELECT * FROM _incr")
                    self._con.unregister("_incr")

                    new_ts = str(df["completed_at"].max())
                    self._save_last_completion_ts(table, new_ts)

                    # Update row count in meta
                    total = self._con.execute(
                        f"SELECT COUNT(*) FROM {table}"
                    ).fetchone()[0]
                    self._upsert_source_meta(table, total)

                    log.info(
                        "[table_cache]   %-*s  +%d new rows appended  "
                        "(total: %d)  new max_ts: %s",
                        col_w, table, len(df), total, new_ts,
                    )
                    log.info("[table_cache] ─────────────────────────────────────────────────────")
                    continue

                # ── Full refresh path (batch by user_id) ─────────────────────
                log.info(
                    "[table_cache] ── %s: full refresh (batch_size=%d) ──────────",
                    table, batch_size,
                )

                user_ids = self._con.execute(f"""
                    SELECT id FROM users
                    WHERE status = 1 AND deleted_at IS NULL
                      AND type IN ({types_sql})
                """).fetchdf()["id"].tolist()

                n_users  = len(user_ids)
                chunks   = [user_ids[i:i + batch_size]
                            for i in range(0, n_users, batch_size)]
                n_chunks = len(chunks)

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
                            self._con.execute(
                                f"CREATE TABLE {table} AS SELECT * FROM _tmp"
                            )
                            table_created = True
                        else:
                            self._con.execute(
                                f"INSERT INTO {table} SELECT * FROM _tmp"
                            )
                        self._con.unregister("_tmp")
                        total_rows += len(df)

                        # Track running MAX(completed_at)
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

                # Apply lesson filter once in DuckDB — uses cached tables,
                # no SSH needed. Much faster than running subquery per batch.
                if table_created:
                    self._apply_lesson_filter_in_duckdb(table)
                    total_rows = self._con.execute(
                        f"SELECT COUNT(*) FROM {table}"
                    ).fetchone()[0]

                self._upsert_source_meta(table, total_rows)

                if max_ts is not None:
                    self._save_last_completion_ts(table, str(max_ts))
                    log.info(
                        "[table_cache]   %-*s  max completed_at saved: %s",
                        col_w, table, max_ts,
                    )

                log.info(
                    "[table_cache]   %-*s  %10d total rows  ✓ cached",
                    col_w, table, total_rows,
                )
                log.info("[table_cache] ─────────────────────────────────────────────────────")

            except Exception as exc:
                log.error("[table_cache]   %-*s  ✗ FAILED: %s", col_w, table, exc)
                raise

        log.info("[table_cache] Completion tables ready — s3 will query DuckDB this run")

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


# ── Result buffer ─────────────────────────────────────────────────────────────

class ResultBuffer:
    """
    Accumulates chunk results in DuckDB during the loop, then streams
    everything to the analytics DB in one go at the end.

    Why: each per-chunk write opens 2 SSH connections (one per table).
    For 635 chunks that is ~1,270 SSH connections just for writes.
    Buffering in DuckDB reduces that to a single bulk write at the end.

    Active for full refresh runs only (start_chunk=1, no --since).
    Falls back to per-chunk writes for --start-chunk resume and --since.

    Streams from DuckDB → analytics in stream_chunk-row batches so the
    full result never has to fit in memory at once.
    """

    _TABLES = {
        "lesson":      "_rbuf_lesson",
        "subject":     "_rbuf_subject",
        "subject_all": "_rbuf_subject_all",
    }

    def __init__(self, con: duckdb.DuckDBPyConnection):
        self._con     = con
        self._created: set[str] = set()

    def append(self, key: str, df: pd.DataFrame):
        """Append one chunk's result to the DuckDB buffer table."""
        if df.empty:
            return
        buf = self._TABLES[key]
        self._con.register("_rbuf", df)
        if buf not in self._created:
            self._con.execute(f"DROP TABLE IF EXISTS {buf}")
            self._con.execute(f"CREATE TABLE {buf} AS SELECT * FROM _rbuf")
            self._created.add(buf)
        else:
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
        Uses fetchmany() so only stream_chunk rows are in memory at once.
        """
        from db import write_table as _write_table

        buf = self._TABLES[key]
        if buf not in self._created:
            log.info("[result_buf] %s — nothing buffered, skipping", analytics_table)
            return 0

        total = self._con.execute(f"SELECT COUNT(*) FROM {buf}").fetchone()[0]
        log.info(
            "[result_buf] flushing %-45s → %s  (%d rows in %d-row batches)",
            buf, analytics_table, total, stream_chunk,
        )

        cursor  = self._con.execute(f"SELECT * FROM {buf}")
        columns = [d[0] for d in cursor.description]
        first   = True
        written = 0

        while True:
            rows = cursor.fetchmany(stream_chunk)
            if not rows:
                break
            df   = pd.DataFrame(rows, columns=columns)
            mode = if_exists if first else "append"
            _write_table(analytics_cfg, df, analytics_table, if_exists=mode)
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

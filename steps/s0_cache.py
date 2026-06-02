"""
Local DuckDB allocation cache and allocation-change detector.

How it works
────────────
First run (or when allocation tables change):
  • Allocation is fetched live from production (SSH → MySQL) — existing logic,
    completely unchanged.
  • Each chunk's allocation result is appended to cache.duckdb.
  • After all chunks, row-count snapshots of the allocation watch tables are saved.

Subsequent runs (allocation tables unchanged):
  • Row counts are compared against the saved snapshot in < 1 second.
  • If nothing changed, each chunk's allocation is loaded from the local DuckDB
    file — no SSH connections needed for allocation queries.
  • Completion data is always fetched live from production (new completions are
    the whole point of running the pipeline).

Cache is only used/built for full, unscoped runs (no --user-id / --centre-id /
--batch-id / --subject-id / --trade-id filters and no --since flag). Scoped
runs always use live allocation — they are fast and this keeps cache logic simple.

Use --force-refresh to bypass the cache and rebuild it from scratch.

Cache file: cache.duckdb  (pipeline root, gitignored)
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

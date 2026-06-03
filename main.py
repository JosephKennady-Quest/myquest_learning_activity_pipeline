"""
AEL V2 Pipeline — User Learning Allocation & Completion
========================================================

Usage:
  # All users — write CSV to ./output/
  python main.py

  # Single user
  python main.py --user-id 04d9c06e-1a37-428e-9b38-c0243b86544d

  # All users in a specific centre
  python main.py --centre-id 0dd48495-0d5f-4663-8b08-a78bc1e2d19c

  # All users in a specific batch
  python main.py --batch-id 096b21a6-bf16-436f-92a5-42b46a01b336

  # Combine filters
  python main.py --centre-id <uuid> --batch-id <uuid>

  # Write to analytics DB instead of (or in addition to) CSV
  python main.py --output db
  python main.py --output both

  # Dry run — print row counts only, no output written
  python main.py --dry-run

Output columns (one row per user × lesson):
  user_id, user_name, user_type, centre_id, project_id,
  batch_id, trade_id, career_path_id, career_path_name,
  subject_id, subject_name, subject_is_ple, ple_career_path_id,
  year_to_map, subject_order,
  lesson_id, lesson_name, lesson_order, lesson_type,
  is_assessment, toolkit_type, allocation_path,
  score, rating, data_from, completed,
  total_allocated, total_completed, completion_pct,
  subj_total_allocated, subj_lessons_allocated, subj_assessments_allocated,
  subj_total_completed, subj_lessons_completed, subj_assessments_completed

Zero-completion users: one stub row per user with lesson/subject fields NULL,
completed = 0, total_completed = 0, completion_pct = 0.0.
"""

import argparse
import gc
import gzip
import logging
import logging.handlers
import os
import shutil
import sys
from datetime import datetime

import pandas as pd

from config import ANALYTICS_DB, ALLOC_CHUNK_SIZE, STAFF_ALLOC_CHUNK_SIZE, OUTPUT_DIR
from db import delete_user_rows, write_table
from steps.s0_cache import AllocationCache, ResultBuffer, TableCache
from steps.s0_changed_users import fetch_changed_user_ids
from steps.s1_users import fetch_users
from steps.s2_allocation import fetch_allocation
from steps.s3_completion import fetch_completion, merge_completion

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("main")

_LOG_FMT = logging.Formatter(
    "%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


def _setup_file_logging(log_path: str) -> None:
    """
    Add a daily-rotating gzip-compressed file handler to the root logger.
    Active log: <log_path>
    Rotated logs: <log_path>.YYYY-MM-DD.gz  (kept for 30 days)
    """
    os.makedirs(os.path.dirname(os.path.abspath(log_path)), exist_ok=True)
    handler = logging.handlers.TimedRotatingFileHandler(
        log_path, when="midnight", backupCount=30, encoding="utf-8",
    )
    handler.setFormatter(_LOG_FMT)

    def _rotator(source, dest):
        with open(source, "rb") as f_in, gzip.open(dest, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)
        os.remove(source)

    handler.rotator = _rotator
    handler.namer   = lambda name: name + ".gz"
    logging.getLogger().addHandler(handler)

# Lesson-level detail table (one row per user × completed lesson)
OUTPUT_TABLE_LESSON      = "main_learning_activity_myquest_ael_lesson"
# Subject-level aggregation — filtered (excludes pdf/mp4/pdf web lessons)
OUTPUT_TABLE_SUBJECT     = "main_learning_activity_myquest_ael"
# Subject-level aggregation — all lesson types (includes pdf/mp4/pdf web)
OUTPUT_TABLE_SUBJECT_ALL = "main_learning_activity_myquest_ael_all_lesson_type"

# Lesson types excluded from the default output (pdf/mp4/pdf web are non-interactive content)
_EXCLUDED_LESSON_TYPES = {"pdf", "mp4", "pdf web"}


def parse_args():
    p = argparse.ArgumentParser(description="AEL V2 — learning allocation + completion pipeline")
    p.add_argument("--user-id",    default=None, help="Filter to a single user UUID")
    p.add_argument("--centre-id",  default=None, help="Filter to a single centre UUID")
    p.add_argument("--batch-id",   default=None, help="Filter to a single batch UUID")
    p.add_argument("--subject-id", default=None, help="Filter to a single subject UUID")
    p.add_argument("--trade-id",   default=None, help="Filter to a single trade UUID")
    p.add_argument(
        "--output",
        choices=["csv", "db", "both"],
        default="db",
        help="Where to write results (default: db)",
    )
    p.add_argument(
        "--outputs",
        default="lesson,subject,debug",
        help="Comma-separated list of outputs to write: lesson,subject,debug (default: all)",
    )
    p.add_argument(
        "--all-lesson-types",
        action="store_true",
        help="Also write outputs that include pdf/mp4/pdf-web lessons (in addition to the filtered default)",
    )
    p.add_argument(
        "--since",
        default=None,
        metavar="DATETIME",
        help=(
            "Incremental mode: only process users with learning_activities.completed_at "
            "after this timestamp. Format: 'YYYY-MM-DD HH:MM:SS'. "
            "Omit for a full refresh (default)."
        ),
    )
    p.add_argument("--dry-run", action="store_true", help="Print row counts only, no output written")
    p.add_argument(
        "--force-refresh",
        action="store_true",
        help=(
            "Bypass the local allocation cache and re-fetch allocation from production. "
            "Rebuilds the cache at the end of the run."
        ),
    )
    p.add_argument(
        "--start-chunk",
        type=int,
        default=1,
        metavar="N",
        help=(
            "Resume a killed run from chunk N (1-based). Skips chunks 1..N-1 and appends "
            "rather than replacing, so already-written chunks are preserved. "
            "Example: --start-chunk 65  (resume after kill at chunk 64)"
        ),
    )
    p.add_argument(
        "--log-file",
        default=None,
        metavar="PATH",
        help=(
            "Write logs to this file in addition to stdout. "
            "Rotates daily at midnight; old files are gzip-compressed automatically. "
            "Example: --log-file /home/joseph/logs/ael_pipeline.log"
        ),
    )
    return p.parse_args()


def _make_tag(
    user_id:    str | None,
    centre_id:  str | None = None,
    batch_id:   str | None = None,
    subject_id: str | None = None,
    trade_id:   str | None = None,
) -> str:
    """Build a descriptive filename tag from whichever filters are active."""
    parts = []
    if user_id:
        parts.append(f"user_{user_id[:8]}")
    if centre_id:
        parts.append(f"ctr_{centre_id[:8]}")
    if batch_id:
        parts.append(f"batch_{batch_id[:8]}")
    if subject_id:
        parts.append(f"subj_{subject_id[:8]}")
    if trade_id:
        parts.append(f"trade_{trade_id[:8]}")
    return "_".join(parts) if parts else "all_users"


def _save_csv(
    df:         pd.DataFrame,
    user_id:    str | None,
    centre_id:  str | None = None,
    batch_id:   str | None = None,
    subject_id: str | None = None,
    trade_id:   str | None = None,
    prefix:     str = "lessons",
) -> str:
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    ts   = datetime.now().strftime("%Y%m%d_%H%M%S")
    tag  = _make_tag(user_id, centre_id, batch_id, subject_id, trade_id)
    path = os.path.join(OUTPUT_DIR, f"{prefix}_{tag}_{ts}.csv")
    df.to_csv(path, index=False)
    log.info("CSV saved → %s", path)
    return path


def _save_allocation_debug(
    df:         pd.DataFrame,
    user_id:    str | None,
    centre_id:  str | None = None,
    batch_id:   str | None = None,
    subject_id: str | None = None,
    trade_id:   str | None = None,
    prefix:     str = "debug_alloc",
) -> None:
    """Save the raw allocation DataFrame (pre-completion) for inspection."""
    cols = [
        "user_id", "user_name", "user_type",
        "centre_id", "batch_id", "trade_id", "career_path_id", "career_path_name",
        "subject_id", "subject_name", "year_to_map", "trade_duration", "subject_order",
        "lesson_id", "lesson_name", "lesson_order", "lesson_type", "is_assessment",
        "toolkit_type", "allocation_path", "allocation_basis",
        "subj_total_allocated", "subj_lessons_allocated", "subj_assessments_allocated",
    ]
    present = [c for c in cols if c in df.columns]
    _save_csv(df[present], user_id, centre_id, batch_id, subject_id, trade_id, prefix=prefix)


def _apply_lesson_type_filter(df: pd.DataFrame) -> pd.DataFrame:
    """Remove lessons whose lesson_type is pdf / mp4 / pdf web (default filter)."""
    if "lesson_type" not in df.columns:
        return df
    mask = df["lesson_type"].fillna("").str.lower().str.strip().isin(_EXCLUDED_LESSON_TYPES)
    return df[~mask].reset_index(drop=True)


def _compute_subj_alloc_counts(df: pd.DataFrame) -> pd.DataFrame:
    """Return per-(user_id, subject_id) allocation counts to merge onto a DataFrame."""
    _ia = pd.to_numeric(df["is_assessment"], errors="coerce").fillna(0).astype(int)
    return (
        df.assign(_ia=_ia)
        .groupby(["user_id", "subject_id"], as_index=False)
        .agg(
            subj_total_allocated     =("lesson_id", "count"),
            subj_lessons_allocated   =("_ia",       lambda x: (x == 0).sum()),
            subj_assessments_allocated=("_ia",      lambda x: (x == 1).sum()),
        )
    )


def _build_subject_agg(df: pd.DataFrame) -> pd.DataFrame:
    """Collapse lesson-level result to one row per (user × subject).
    Rows without a subject_id (zero-allocation / zero-completion stubs) are excluded."""
    if "subject_id" not in df.columns or df.empty:
        return pd.DataFrame()
    df = df[df["subject_id"].notna()].copy()
    if df.empty:
        return pd.DataFrame()
    identity = [
        "user_id", "user_name", "user_type", "centre_id", "project_id",
        "batch_id", "trade_id", "career_path_id", "career_path_name",
        "subject_id", "subject_name", "subject_is_ple", "year_to_map", "allocation_basis",
        "total_allocated", "total_lessons_allocated", "total_assessments_allocated",
        "total_completed", "total_lessons_completed", "total_assessments_completed",
        "completion_pct",
        "subj_total_allocated", "subj_lessons_allocated", "subj_assessments_allocated",
        "subj_total_completed", "subj_lessons_completed", "subj_assessments_completed",
    ]
    first_cols = [c for c in identity if c in df.columns and c not in ("user_id", "subject_id")]
    agg = (
        df.groupby(["user_id", "subject_id"], as_index=False, sort=False)
        .agg(
            **{c: (c, "first") for c in first_cols},
            avg_score     =("score",    "mean"),
            avg_rating    =("rating",   "mean"),
            **( {"avg_duration":   ("duration", "mean"),
                  "total_duration": ("duration", "sum")}
                if "duration" in df.columns else {} ),
        )
    )
    agg["avg_score"]  = pd.to_numeric(agg["avg_score"],  errors="coerce").round(2)
    agg["avg_rating"] = pd.to_numeric(agg["avg_rating"], errors="coerce").round(2)
    if "avg_duration" in agg.columns:
        agg["avg_duration"] = pd.to_numeric(agg["avg_duration"], errors="coerce").round(2)
    return agg


def _print_summary(df: pd.DataFrame) -> None:
    if df.empty:
        log.warning("Result is empty — check DB connections and user filters.")
        return

    n_users    = df["user_id"].nunique()
    n_subjects = df["subject_id"].nunique() if "subject_id" in df.columns else 0
    n_lessons  = df["lesson_id"].nunique()  if "lesson_id"  in df.columns else 0
    n_ple      = df.loc[df["allocation_path"] == "ple",     "user_id"].nunique() if "allocation_path" in df.columns else 0
    n_non_ple  = df.loc[df["allocation_path"] == "non_ple", "user_id"].nunique() if "allocation_path" in df.columns else 0
    avg_pct    = df.drop_duplicates("user_id")["completion_pct"].mean() if "completion_pct" in df.columns else 0

    print("\n" + "─" * 60)
    print(f"  Users processed       : {n_users:,}  (PLE: {n_ple} | non-PLE: {n_non_ple})")
    print(f"  Unique subjects       : {n_subjects:,}")
    print(f"  Unique lessons        : {n_lessons:,}")
    print(f"  Total rows            : {len(df):,}")
    print(f"  Avg completion        : {avg_pct:.1f}%")
    print("─" * 60 + "\n")


def _print_summary_chunked(summary_df: pd.DataFrame, total_fetched: int) -> None:
    """Print final summary from accumulated per-user rows (used in multi-chunk runs)."""
    n_users   = summary_df["user_id"].nunique()
    n_ple     = summary_df.loc[summary_df.get("allocation_path", pd.Series()) == "ple",     "user_id"].nunique() if "allocation_path" in summary_df.columns else 0
    n_non_ple = summary_df.loc[summary_df.get("allocation_path", pd.Series()) == "non_ple", "user_id"].nunique() if "allocation_path" in summary_df.columns else 0
    n_no_alloc = n_users - n_ple - n_non_ple
    avg_pct   = summary_df.drop_duplicates("user_id")["completion_pct"].mean() if "completion_pct" in summary_df.columns else 0

    print("\n" + "─" * 60)
    print(f"  Users fetched         : {total_fetched:,}")
    print(f"  Users in output       : {n_users:,}  (PLE: {n_ple} | non-PLE: {n_non_ple} | no-alloc: {n_no_alloc})")
    print(f"  Avg completion        : {avg_pct:.1f}%")
    print("─" * 60 + "\n")


def run(
    user_id:          str | None = None,
    centre_id:        str | None = None,
    batch_id:         str | None = None,
    subject_id:       str | None = None,
    trade_id:         str | None = None,
    since:            str | None = None,
    output:           str = "db",
    dry_run:          bool = False,
    outputs:          str = "lesson,subject,debug",
    all_lesson_types: bool = False,
    start_chunk:      int = 1,
    force_refresh:    bool = False,
) -> pd.DataFrame:
    active = {o.strip().lower() for o in outputs.split(",")}
    mode   = f"incremental (since={since})" if since else "full refresh"

    log.info("═" * 60)
    log.info(
        "AEL V2 pipeline starting  "
        "[user_id=%s | centre_id=%s | batch_id=%s | subject_id=%s | trade_id=%s"
        " | since=%s | output=%s | outputs=%s | dry_run=%s]",
        user_id or "ALL", centre_id or "ALL", batch_id or "ALL",
        subject_id or "ALL", trade_id or "ALL",
        since or "NONE (full refresh)",
        output, outputs, dry_run,
    )
    log.info("═" * 60)

    # ── Step 0 (incremental only): scope to users with new completions ────────
    # When --since is provided, query learning_activities.completed_at to find
    # only users who have at least one new completion since that timestamp.
    # All other users are skipped — their existing rows in the DB are untouched.
    changed_ids: set | None = None
    if since:
        ids = fetch_changed_user_ids(since)
        if not ids:
            log.info("Incremental run: no new completions since %s — nothing to update.", since)
            return pd.DataFrame()
        changed_ids = set(ids)
        log.info("Incremental run: %d users with new completions since %s", len(changed_ids), since)

    # ── Local allocation cache ────────────────────────────────────────────────
    # Cache is only used for full, unscoped runs (no filters, no --since).
    # Scoped runs are fast enough to always fetch live; --since always needs
    # live allocation in case allocation changed for those users.
    _scoped         = any([user_id, centre_id, batch_id, subject_id, trade_id])
    # Cache is used for all full unscoped runs — including --force-refresh.
    # force_refresh controls whether to REUSE existing cached data, not
    # whether to use caching at all. Excluding it here was a bug that caused
    # cache.duckdb to never be built when --force-refresh was passed.
    _cache_eligible = not _scoped and not since

    cache: AllocationCache | None = None
    _alloc_from_cache = False
    _cache_total_rows = 0

    # fetch_fn is passed to s1 + s2 so they query DuckDB instead of MySQL.
    # None means fall back to the normal db.fetch (production MySQL).
    fetch_fn = None

    if _cache_eligible:
        cache = AllocationCache()
        tbl   = TableCache(cache._con)

        # ── Layer 1: source table cache ───────────────────────────────────────
        alloc_changed = cache.allocation_changed()

        if alloc_changed or not tbl.is_fresh() or force_refresh:
            log.info("[table_cache] Refreshing source tables from production ...")
            tbl.refresh()
        else:
            tbl.log_status()

        # Refresh completion tables every run (live data).
        # incremental=True (default): appends only rows newer than last run.
        # incremental=False (--force-refresh): drops and rebuilds from scratch.
        tbl.refresh_completion_tables(incremental=not force_refresh)

        fetch_fn = tbl.make_fetch_fn()
        log.info("[table_cache] s1 + s2 + s3 will all query DuckDB cache this run")

        # ── Layer 2: allocation result cache ──────────────────────────────────
        if not alloc_changed and cache.is_ready() and not force_refresh:
            _alloc_from_cache = True
            log.info("[cache] Allocation result cache valid — chunk allocation loads from DuckDB")
        else:
            cache.reset()
            log.info("[cache] Allocation result cache will be rebuilt this run")

    # ── Result buffer ─────────────────────────────────────────────────────────
    # Buffer chunk results in DuckDB, flush to analytics DB once at the end.
    # Eliminates ~2 SSH write connections per chunk (saves ~95 min on 635 chunks).
    # Only active for full unscoped runs starting from chunk 1.
    # Falls back to per-chunk writes for --start-chunk resume and --since.
    _use_result_buf = (
        cache is not None          # DuckDB is active
        and not since              # not incremental
        and start_chunk == 1       # not resuming a partial run
        and output in ("db", "both")
    )
    result_buf: ResultBuffer | None = ResultBuffer(cache._con) if _use_result_buf else None
    if _use_result_buf:
        log.info("[result_buf] Result buffering ON — analytics DB writes deferred to end of run")

    # ── Step 1: fetch users (lightweight — demographics only)
    users_df = fetch_users(user_id, centre_id, batch_id, trade_id, fetch_fn=fetch_fn)
    if users_df.empty:
        log.warning("No users found — exiting.")
        return pd.DataFrame()

    # Apply incremental scope filter to users_df
    if changed_ids is not None:
        users_df = users_df[users_df["user_id"].isin(changed_ids)].reset_index(drop=True)
        if users_df.empty:
            log.info("None of the changed users match the current filters — nothing to update.")
            return pd.DataFrame()
        log.info("Scoped to %d users matching both filters and new-completion list", len(users_df))

    # ── Separate learners (3,4) from staff (1,2) for independent chunking.
    # Staff (especially Admin) return many more rows per user, so they use a
    # smaller chunk size and only run the staff allocation path — not non_ple/ple.
    # Learner chunks only run non_ple + ple — the staff query is never called
    # for learner-only chunks, restoring the original 2-queries-per-chunk cadence.
    learner_ids = users_df.loc[users_df["user_type"].isin([3, 4]), "user_id"].dropna().tolist()
    staff_ids   = users_df.loc[users_df["user_type"].isin([1, 2]), "user_id"].dropna().tolist()

    learner_chunks = [learner_ids[i:i + ALLOC_CHUNK_SIZE]
                      for i in range(0, len(learner_ids), ALLOC_CHUNK_SIZE)]
    staff_chunks   = [staff_ids[i:i + STAFF_ALLOC_CHUNK_SIZE]
                      for i in range(0, len(staff_ids), STAFF_ALLOC_CHUNK_SIZE)]

    all_chunks = [(c, "learner") for c in learner_chunks] + [(c, "staff") for c in staff_chunks]
    n_chunks   = len(all_chunks)

    log.info(
        "Users: %d learners (%d chunks) + %d staff (%d chunks) = %d chunks total",
        len(learner_ids), len(learner_chunks),
        len(staff_ids),   len(staff_chunks),
        n_chunks,
    )

    # Small run = each user type fits in one chunk. Used to gate debug CSV.
    is_small_run = (len(learner_chunks) <= 1 and len(staff_chunks) <= 1)

    if start_chunk > 1:
        log.info("Resuming from chunk %d — chunks 1..%d already written; appending only.",
                 start_chunk, start_chunk - 1)

    # When resuming, treat every write as append (don't truncate already-written data).
    first_write     = (start_chunk == 1)
    first_all_write = (start_chunk == 1)

    summary_rows  = []   # lightweight per-user rows for final summary
    no_alloc_rows = []   # no-allocation users accumulated across chunks

    # For --all-lesson-types CSV on small runs: accumulate subject-level frames only
    # (subject agg is ~24K rows/chunk — manageable). Large runs skip the CSV.
    all_type_subj_frames: list = [] if (all_lesson_types and is_small_run) else None
    _no_alloc_keep = [c for c in [
        "user_id", "user_name", "user_type", "is_master_trainer",
        "centre_id", "project_id", "is_ple", "batch_id", "trade_id",
    ] if c in users_df.columns]

    for chunk_idx, (chunk_ids, chunk_type) in enumerate(all_chunks, 1):
        # ── Resume: skip already-processed chunks ─────────────────────────────
        if chunk_idx < start_chunk:
            if chunk_idx % 50 == 0 or chunk_idx == start_chunk - 1:
                log.info("Skipping chunk %d / %d (already written)", chunk_idx, n_chunks)
            continue

        if n_chunks > 1:
            log.info("─── Chunk %d / %d  (%d %s users) ───",
                     chunk_idx, n_chunks, len(chunk_ids), chunk_type)

        # ── Step 2: allocation for this chunk ────────────────────────────────
        alloc_paths = ("non_ple", "ple") if chunk_type == "learner" else ("staff",)

        if _alloc_from_cache:
            # Load from local DuckDB — no SSH connection needed for allocation.
            try:
                alloc = cache.load_chunk(chunk_ids)
                log.info("[cache] Chunk %d allocation → DuckDB (%d rows)", chunk_idx, len(alloc))
            except Exception as exc:
                log.warning("[cache] Cache read failed (%s) — falling back to live query", exc)
                alloc = fetch_allocation(
                    user_ids=chunk_ids,
                    centre_id=centre_id, batch_id=batch_id,
                    subject_id=subject_id, trade_id=trade_id,
                    paths=alloc_paths,
                )
                log.info("[cache] Chunk %d allocation → production (cache fallback)", chunk_idx)
        else:
            alloc = fetch_allocation(
                user_ids=chunk_ids,
                centre_id=centre_id, batch_id=batch_id,
                subject_id=subject_id, trade_id=trade_id,
                paths=alloc_paths,
                fetch_fn=fetch_fn,
            )
            src = "DuckDB" if fetch_fn else "production"
            log.info("[cache] Chunk %d allocation → %s (caching result for next run)", chunk_idx, src)
            # Populate the cache while we go, so subsequent runs can use it.
            if cache is not None and not alloc.empty:
                cache.append(alloc)
                _cache_total_rows += len(alloc)
        alloc_filtered = _apply_lesson_type_filter(alloc) if not alloc.empty else alloc

        if not alloc.empty:
            log.info(
                "Lesson type filter: %d → %d rows (%d pdf/mp4/pdf-web excluded)",
                len(alloc), len(alloc_filtered), len(alloc) - len(alloc_filtered),
            )

        # Debug CSV — only for small (single-chunk) runs and when CSV output is requested
        if "debug" in active and not dry_run and output in ("csv", "both") and is_small_run and not alloc_filtered.empty:
            counts = _compute_subj_alloc_counts(alloc_filtered)
            _save_allocation_debug(
                alloc_filtered.merge(counts, on=["user_id", "subject_id"], how="left"),
                user_id, centre_id, batch_id, subject_id, trade_id,
            )
            if all_lesson_types:
                counts_all = _compute_subj_alloc_counts(alloc)
                _save_allocation_debug(
                    alloc.merge(counts_all, on=["user_id", "subject_id"], how="left"),
                    user_id, centre_id, batch_id, subject_id, trade_id,
                    prefix="debug_alloc_all_types",
                )

        # ── Step 3: completion ────────────────────────────────────────────────
        if not alloc.empty:
            u_types = set(alloc["user_type"].dropna().astype(int).unique())
            a_ids   = alloc["user_id"].dropna().unique().tolist()
            log.info("Fetching completion for %d allocated users", len(a_ids))
            compl  = fetch_completion(user_ids=a_ids, user_types=u_types, fetch_fn=fetch_fn)
            result = merge_completion(alloc_filtered, compl, fetch_fn=fetch_fn)
            if all_lesson_types:
                # Compute all-types output only when explicitly requested.
                # Reuse completion data, so this adds no extra DB read.
                result_all  = merge_completion(alloc, compl, fetch_fn=fetch_fn)
                subj_all_df = _build_subject_agg(result_all)
                del result_all   # free lesson-level all-types immediately
            else:
                subj_all_df = pd.DataFrame()
        else:
            compl       = None
            result      = pd.DataFrame()
            subj_all_df = pd.DataFrame()

        # ── Add stubs for users in this chunk with no allocation ──────────────
        result_uids  = set(result["user_id"].dropna().unique()) if not result.empty else set()
        no_alloc_ids = set(chunk_ids) - result_uids
        if no_alloc_ids:
            stub = (
                users_df[users_df["user_id"].isin(no_alloc_ids)]
                [_no_alloc_keep]
                .copy()
            )
            stub["total_allocated"]             = 0
            stub["total_lessons_allocated"]     = 0
            stub["total_assessments_allocated"] = 0
            stub["total_completed"]             = 0
            stub["total_lessons_completed"]     = 0
            stub["total_assessments_completed"] = 0
            stub["completion_pct"]              = 0.0
            stub["completed"]                   = 0
            result = pd.concat([result, stub], ignore_index=True, sort=False) if not result.empty else stub
            log.info("Added %d users with no allocation (stub rows)", len(stub))
            no_alloc_rows.append(stub[_no_alloc_keep].copy())

        if result.empty:
            del alloc, alloc_filtered
            gc.collect()
            continue

        # Accumulate lightweight per-user summary (3 cols × n_users — stays small)
        per_user_cols = [c for c in ["user_id", "allocation_path", "completion_pct"]
                         if c in result.columns]
        summary_rows.append(
            result.drop_duplicates("user_id")[per_user_cols].copy()
        )

        # Print detailed summary for single-chunk runs; skip per-chunk for multi
        if n_chunks == 1:
            _print_summary(result)

        if dry_run:
            del alloc, alloc_filtered, result, subj_all_df
            gc.collect()
            continue

        # ── Write / buffer outputs ────────────────────────────────────────────
        subject_agg_df = _build_subject_agg(result)

        if output in ("db", "both"):
            if since:
                # Incremental: per-chunk delete + insert (unchanged behaviour)
                delete_user_rows(ANALYTICS_DB, OUTPUT_TABLE_LESSON,  chunk_ids)
                delete_user_rows(ANALYTICS_DB, OUTPUT_TABLE_SUBJECT, chunk_ids)
                if all_lesson_types:
                    delete_user_rows(ANALYTICS_DB, OUTPUT_TABLE_SUBJECT_ALL, chunk_ids)
                db_write_mode     = "append"
                db_write_mode_all = "append"
            else:
                db_write_mode     = "replace" if first_write     else "append"
                db_write_mode_all = "replace" if first_all_write else "append"

        if "lesson" in active:
            if output in ("csv", "both"):
                _save_csv(result, user_id, centre_id, batch_id, subject_id, trade_id,
                          prefix="lessons_filtered")
            if output in ("db", "both"):
                if result_buf:
                    result_buf.append("lesson", result)
                    log.info("[result_buf] Chunk %d buffered → lesson  (%d rows, total: %d)",
                             chunk_idx, len(result), result_buf.row_count("lesson"))
                else:
                    write_table(ANALYTICS_DB, result, OUTPUT_TABLE_LESSON, if_exists=db_write_mode)
                    log.info("DB written → %s  (chunk %d/%d, mode=%s)",
                             OUTPUT_TABLE_LESSON, chunk_idx, n_chunks, db_write_mode)

        if "subject" in active:
            if output in ("csv", "both"):
                _save_csv(subject_agg_df, user_id, centre_id, batch_id, subject_id, trade_id,
                          prefix="subjects_filtered")
            if output in ("db", "both"):
                if result_buf:
                    result_buf.append("subject", subject_agg_df)
                    log.info("[result_buf] Chunk %d buffered → subject  (%d rows, total: %d)",
                             chunk_idx, len(subject_agg_df), result_buf.row_count("subject"))
                else:
                    write_table(ANALYTICS_DB, subject_agg_df, OUTPUT_TABLE_SUBJECT,
                                if_exists=db_write_mode)
                    log.info("DB written → %s  (chunk %d/%d, mode=%s)",
                             OUTPUT_TABLE_SUBJECT, chunk_idx, n_chunks, db_write_mode)

            # All-lesson-types subject table
            if all_lesson_types and output in ("db", "both") and not subj_all_df.empty:
                if result_buf:
                    result_buf.append("subject_all", subj_all_df)
                else:
                    write_table(ANALYTICS_DB, subj_all_df, OUTPUT_TABLE_SUBJECT_ALL,
                                if_exists=db_write_mode_all)
                    log.info("DB written → %s  (chunk %d/%d, mode=%s)",
                             OUTPUT_TABLE_SUBJECT_ALL, chunk_idx, n_chunks, db_write_mode_all)

            # Accumulate all-types subject rows for CSV on small runs only
            if all_type_subj_frames is not None and not subj_all_df.empty:
                all_type_subj_frames.append(subj_all_df.copy())

        first_write     = False
        first_all_write = False

        # ── Release memory before next chunk ─────────────────────────────────
        del alloc, alloc_filtered, compl, result, subject_agg_df, subj_all_df
        gc.collect()

    # ── Flush result buffer to analytics DB (one bulk write) ─────────────────
    if result_buf is not None and not dry_run:
        log.info("[result_buf] ── Flushing all buffered results to analytics DB ──────")
        if "lesson" in active:
            result_buf.flush("lesson", ANALYTICS_DB, OUTPUT_TABLE_LESSON, if_exists="replace")
        if "subject" in active:
            result_buf.flush("subject", ANALYTICS_DB, OUTPUT_TABLE_SUBJECT, if_exists="replace")
            if all_lesson_types:
                result_buf.flush("subject_all", ANALYTICS_DB, OUTPUT_TABLE_SUBJECT_ALL,
                                 if_exists="replace")
        log.info("[result_buf] ── Flush complete ────────────────────────────────────")

    # ── Finalise allocation cache (only when we just rebuilt it) ─────────────
    if cache is not None:
        if not _alloc_from_cache and _cache_total_rows > 0:
            cache.finalise(_cache_total_rows)
            cache.save_row_count_snapshot()
            log.info("[cache] Allocation cache rebuilt — %d rows saved to %s", _cache_total_rows, cache.path)
        cache.close()

    # ── All-lesson-types CSV (small runs only) ────────────────────────────────
    if all_lesson_types and not dry_run and output in ("csv", "both"):
        if all_type_subj_frames:
            combined_all = pd.concat(all_type_subj_frames, ignore_index=True)
            if "subject" in active:
                _save_csv(combined_all, user_id, centre_id, batch_id,
                          subject_id, trade_id, prefix="subjects_all_types")
            del combined_all
        elif not is_small_run:
            log.info(
                "Skipping all-types CSV for large run (%d chunks) — "
                "rerun with narrower filters if an all-types CSV is required.",
                n_chunks,
            )

    # ── Save no-allocation user list (CSV only) ───────────────────────────────
    if no_alloc_rows and not dry_run and output in ("csv", "both"):
        no_alloc_df = pd.concat(no_alloc_rows, ignore_index=True).drop_duplicates("user_id")
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        ts            = datetime.now().strftime("%Y%m%d_%H%M%S")
        no_alloc_path = os.path.join(OUTPUT_DIR, f"no_allocation_users_{ts}.csv")
        no_alloc_df.to_csv(no_alloc_path, index=False)
        log.info(
            "No-allocation users saved → %s  (%d users — had completions but no current allocation)",
            no_alloc_path, len(no_alloc_df),
        )

    # ── Final summary ─────────────────────────────────────────────────────────
    if summary_rows:
        if n_chunks > 1:
            _print_summary_chunked(pd.concat(summary_rows, ignore_index=True), len(learner_ids) + len(staff_ids))
    else:
        log.warning("No output rows produced — check DB connections and filters.")

    if dry_run:
        log.info("Dry run — no output written.")
    else:
        log.info("Pipeline complete.")

    return pd.DataFrame()


if __name__ == "__main__":
    args = parse_args()
    if args.log_file:
        _setup_file_logging(args.log_file)
    df   = run(
        user_id=args.user_id,
        centre_id=args.centre_id,
        batch_id=args.batch_id,
        subject_id=args.subject_id,
        trade_id=args.trade_id,
        since=args.since,
        output=args.output,
        dry_run=args.dry_run,
        outputs=args.outputs,
        all_lesson_types=args.all_lesson_types,
        start_chunk=args.start_chunk,
        force_refresh=args.force_refresh,
    )
    sys.exit(0 if not df.empty else 1)

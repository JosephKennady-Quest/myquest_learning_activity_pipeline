"""
AEL V2 Pipeline — User Learning Allocation & Completion
========================================================

Optimisations in this version
──────────────────────────────
Opt 1  — precompute_allocation(): all 3 allocation JOINs run once for all
         users; each chunk does a fast indexed scan.  ~10 h → ~20 min.

Opt 2  — Parallel chunk processing via ThreadPoolExecutor (CHUNK_WORKERS).
         Each worker fetches allocation + runs merge_completion independently.
         DuckDB reads are concurrent-safe; a write lock guards buffer appends.

Opt 3  — TunnelPool: one persistent SSH tunnel per DB config reused across
         all fetch() / write_table() calls.  Eliminates ~1,270 tunnel
         open/close cycles in a 635-chunk run.

Opt 4  — Batch completion: fetch ALL users' completion from DuckDB in one
         query instead of per-chunk.  Completion is then sliced per chunk
         from an in-memory dict — zero SSH cost for completion.

Opt 5  — ResultBuffer: all chunk results buffered in DuckDB, flushed once
         at the end using a single persistent connection.

Opt 7  — run() split into named stages: setup_cache, build_chunks,
         process_chunks, flush_outputs, finalise.

Opt 10 — Auto-checkpoint: last written chunk saved to cache_meta.  A killed
         run auto-resumes from the checkpoint without --start-chunk.

Usage:
  python main.py                          # all users
  python main.py --user-id <uuid>
  python main.py --centre-id <uuid>
  python main.py --output db              # default
  python main.py --output both            # CSV + DB
  python main.py --dry-run
  python main.py --force-refresh
  python main.py --since 'YYYY-MM-DD HH:MM:SS'
  python main.py --start-chunk 65         # manual resume
"""

import argparse
import gc
import gzip
import logging
import logging.handlers
import os
import shutil
import sys
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Optional

import pandas as pd

from config import (
    ANALYTICS_DB, ALLOC_CHUNK_SIZE, CHUNK_WORKERS,
    LEARNER_TYPES_SQL, OUTPUT_DIR, STAFF_ALLOC_CHUNK_SIZE,
)
from db import TunnelPool, delete_user_rows, write_table
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

# ── Output table names ────────────────────────────────────────────────────────
OUTPUT_TABLE_LESSON      = "main_learning_activity_myquest_ael_lesson"
OUTPUT_TABLE_SUBJECT     = "main_learning_activity_myquest_ael"
OUTPUT_TABLE_SUBJECT_ALL = "main_learning_activity_myquest_ael_all_lesson_type"

_EXCLUDED_LESSON_TYPES = {"pdf", "mp4", "pdf web"}


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────

def parse_args():
    p = argparse.ArgumentParser(description="AEL V2 — learning allocation + completion pipeline")
    p.add_argument("--user-id",    default=None)
    p.add_argument("--centre-id",  default=None)
    p.add_argument("--batch-id",   default=None)
    p.add_argument("--subject-id", default=None)
    p.add_argument("--trade-id",   default=None)
    p.add_argument("--output",     choices=["csv", "db", "both"], default="db")
    p.add_argument("--outputs",    default="lesson,subject,debug")
    p.add_argument("--all-lesson-types", action="store_true")
    p.add_argument(
        "--since", default=None, metavar="DATETIME",
        help="Incremental mode: only users with completed_at after this timestamp.",
    )
    p.add_argument("--dry-run",       action="store_true")
    p.add_argument("--force-refresh", action="store_true")
    p.add_argument(
        "--start-chunk", type=int, default=None, metavar="N",
        help=(
            "Resume from chunk N (1-based).  If omitted, auto-resumes from "
            "the saved checkpoint when one exists."
        ),
    )
    p.add_argument("--log-file", default=None, metavar="PATH")
    p.add_argument(
        "--workers", type=int, default=None,
        help=f"Override CHUNK_WORKERS (default: {CHUNK_WORKERS})",
    )
    return p.parse_args()


def _setup_file_logging(log_path: str) -> None:
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


# ─────────────────────────────────────────────────────────────────────────────
# Pure helpers (unchanged logic, just extracted)
# ─────────────────────────────────────────────────────────────────────────────

def _make_tag(user_id, centre_id=None, batch_id=None, subject_id=None, trade_id=None) -> str:
    parts = []
    if user_id:    parts.append(f"user_{user_id[:8]}")
    if centre_id:  parts.append(f"ctr_{centre_id[:8]}")
    if batch_id:   parts.append(f"batch_{batch_id[:8]}")
    if subject_id: parts.append(f"subj_{subject_id[:8]}")
    if trade_id:   parts.append(f"trade_{trade_id[:8]}")
    return "_".join(parts) if parts else "all_users"


def _save_csv(df, user_id, centre_id=None, batch_id=None,
              subject_id=None, trade_id=None, prefix="lessons") -> str:
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    ts   = datetime.now().strftime("%Y%m%d_%H%M%S")
    tag  = _make_tag(user_id, centre_id, batch_id, subject_id, trade_id)
    path = os.path.join(OUTPUT_DIR, f"{prefix}_{tag}_{ts}.csv")
    df.to_csv(path, index=False)
    log.info("CSV saved → %s", path)
    return path


def _apply_lesson_type_filter(df: pd.DataFrame) -> pd.DataFrame:
    if "lesson_type" not in df.columns:
        return df
    mask = df["lesson_type"].fillna("").str.lower().str.strip().isin(_EXCLUDED_LESSON_TYPES)
    return df[~mask].reset_index(drop=True)


def _compute_subj_alloc_counts(df: pd.DataFrame) -> pd.DataFrame:
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
            avg_score  =("score",  "mean"),
            avg_rating =("rating", "mean"),
            **( {"avg_duration": ("duration", "mean"), "total_duration": ("duration", "sum")}
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
    n_users   = df["user_id"].nunique()
    n_subjects = df["subject_id"].nunique() if "subject_id" in df.columns else 0
    n_lessons  = df["lesson_id"].nunique()  if "lesson_id"  in df.columns else 0
    n_ple     = df.loc[df.get("allocation_path", pd.Series()) == "ple",     "user_id"].nunique() if "allocation_path" in df.columns else 0
    n_non_ple = df.loc[df.get("allocation_path", pd.Series()) == "non_ple", "user_id"].nunique() if "allocation_path" in df.columns else 0
    avg_pct   = df.drop_duplicates("user_id")["completion_pct"].mean() if "completion_pct" in df.columns else 0
    print("\n" + "─" * 60)
    print(f"  Users processed  : {n_users:,}  (PLE: {n_ple} | non-PLE: {n_non_ple})")
    print(f"  Unique subjects  : {n_subjects:,}")
    print(f"  Unique lessons   : {n_lessons:,}")
    print(f"  Total rows       : {len(df):,}")
    print(f"  Avg completion   : {avg_pct:.1f}%")
    print("─" * 60 + "\n")


def _print_summary_chunked(summary_df: pd.DataFrame, total_fetched: int) -> None:
    n_users    = summary_df["user_id"].nunique()
    n_ple      = summary_df.loc[summary_df.get("allocation_path", pd.Series()) == "ple",     "user_id"].nunique() if "allocation_path" in summary_df.columns else 0
    n_non_ple  = summary_df.loc[summary_df.get("allocation_path", pd.Series()) == "non_ple", "user_id"].nunique() if "allocation_path" in summary_df.columns else 0
    n_no_alloc = n_users - n_ple - n_non_ple
    avg_pct    = summary_df.drop_duplicates("user_id")["completion_pct"].mean() if "completion_pct" in summary_df.columns else 0
    print("\n" + "─" * 60)
    print(f"  Users fetched    : {total_fetched:,}")
    print(f"  Users in output  : {n_users:,}  (PLE: {n_ple} | non-PLE: {n_non_ple} | no-alloc: {n_no_alloc})")
    print(f"  Avg completion   : {avg_pct:.1f}%")
    print("─" * 60 + "\n")


# ─────────────────────────────────────────────────────────────────────────────
# Stage helpers
# ─────────────────────────────────────────────────────────────────────────────

def _setup_cache(force_refresh: bool, since: Optional[str], scoped: bool):
    """
    Stage 1 — initialise DuckDB cache layers.

    Returns (cache, tbl, fetch_fn, alloc_precomputed, all_completion_df).
    Returns (None, None, None, False, None) for scoped / incremental runs.
    """
    _cache_eligible = not scoped and not since

    if not _cache_eligible:
        return None, None, None, False, None

    cache = AllocationCache()
    tbl   = TableCache(cache._con)

    alloc_changed = cache.allocation_changed()

    if alloc_changed or not tbl.is_fresh() or force_refresh:
        log.info("[table_cache] Refreshing source tables from production ...")
        tbl.refresh()
    else:
        tbl.log_status()

    tbl.build_indexes()

    # Refresh completion tables (incremental unless --force-refresh)
    tbl.refresh_completion_tables(incremental=not force_refresh)

    fetch_fn = tbl.make_fetch_fn()
    log.info("[table_cache] s1 + s2 + s3 will query DuckDB cache this run")

    # Two-stage precompute: build user×subject map (~21M rows, ~5 min),
    # then expand to lessons per-chunk (~1-2s each).
    if not tbl.user_subject_map_exists():
        tbl.precompute_user_subject_map(LEARNER_TYPES_SQL)
    else:
        log.info("[table_cache] _user_subject_map already built — reusing")
    alloc_precomputed = tbl.user_subject_map_exists()

    # Opt 4 — fetch all completion in one DuckDB query
    all_completion_df = tbl.fetch_all_completion()

    return cache, tbl, fetch_fn, alloc_precomputed, all_completion_df


def _build_chunks(users_df: pd.DataFrame):
    """
    Stage 2 — split users into learner and staff chunks.
    Returns (learner_chunks, staff_chunks, all_chunks).
    """
    learner_ids = users_df.loc[users_df["user_type"].isin([3, 4]), "user_id"].dropna().tolist()
    staff_ids   = users_df.loc[users_df["user_type"].isin([1, 2]), "user_id"].dropna().tolist()

    learner_chunks = [learner_ids[i:i + ALLOC_CHUNK_SIZE]
                      for i in range(0, len(learner_ids), ALLOC_CHUNK_SIZE)]
    staff_chunks   = [staff_ids[i:i + STAFF_ALLOC_CHUNK_SIZE]
                      for i in range(0, len(staff_ids), STAFF_ALLOC_CHUNK_SIZE)]

    all_chunks = [(c, "learner") for c in learner_chunks] + [(c, "staff") for c in staff_chunks]

    log.info(
        "Users: %d learners (%d chunks) + %d staff (%d chunks) = %d chunks total",
        len(learner_ids), len(learner_chunks),
        len(staff_ids),   len(staff_chunks),
        len(all_chunks),
    )
    return learner_chunks, staff_chunks, all_chunks


def _process_one_chunk(
    chunk_idx:         int,
    chunk_ids:         list,
    chunk_type:        str,
    n_chunks:          int,
    users_df:          pd.DataFrame,
    cache,
    tbl,
    fetch_fn,
    alloc_precomputed: bool,
    all_completion_df: Optional[pd.DataFrame],
    centre_id, batch_id, subject_id, trade_id,
    all_lesson_types:  bool,
    no_alloc_keep:     list,
    _alloc_from_cache: bool,
    _cache_total_rows_lock: threading.Lock,
    _cache_total_rows_ref:  list,  # [int] — mutable reference
) -> dict:
    """
    Process one chunk: allocation → completion merge → return result dicts.

    Returns a dict with keys:
        result          pd.DataFrame
        subj_all_df     pd.DataFrame
        alloc           pd.DataFrame  (for cache.append, before filter)
        no_alloc_ids    set
    """
    alloc_paths = ("non_ple", "ple") if chunk_type == "learner" else ("staff",)

    # ── Allocation ────────────────────────────────────────────────────────────
    if alloc_precomputed and tbl is not None:
        alloc = tbl.load_alloc_for_chunk(chunk_ids, chunk_type)
        log.debug("[chunk %d] alloc → _user_subject_map (%d rows)", chunk_idx, len(alloc))
    elif _alloc_from_cache and cache is not None:
        try:
            alloc = cache.load_chunk(chunk_ids)
            log.debug("[chunk %d] alloc → allocation_cache (%d rows)", chunk_idx, len(alloc))
        except Exception as exc:
            log.warning("[cache] Chunk %d cache read failed (%s) — live fallback", chunk_idx, exc)
            alloc = fetch_allocation(
                user_ids=chunk_ids, centre_id=centre_id, batch_id=batch_id,
                subject_id=subject_id, trade_id=trade_id, paths=alloc_paths,
            )
    else:
        alloc = fetch_allocation(
            user_ids=chunk_ids, centre_id=centre_id, batch_id=batch_id,
            subject_id=subject_id, trade_id=trade_id, paths=alloc_paths,
            fetch_fn=fetch_fn,
        )
        if cache is not None and not alloc.empty:
            with _cache_total_rows_lock:
                cache.append(alloc)
                _cache_total_rows_ref[0] += len(alloc)

    alloc_filtered = _apply_lesson_type_filter(alloc) if not alloc.empty else alloc

    # ── Completion ────────────────────────────────────────────────────────────
    if not alloc.empty:
        if all_completion_df is not None and not all_completion_df.empty:
            # Opt 4 — slice the pre-fetched all-user completion
            chunk_uid_set = set(chunk_ids)
            compl = all_completion_df[
                all_completion_df["user_id"].isin(chunk_uid_set)
            ].reset_index(drop=True)
        else:
            u_types = set(alloc["user_type"].dropna().astype(int).unique())
            a_ids   = alloc["user_id"].dropna().unique().tolist()
            compl   = fetch_completion(user_ids=a_ids, user_types=u_types, fetch_fn=fetch_fn)

        result = merge_completion(alloc_filtered, compl, fetch_fn=fetch_fn)

        if all_lesson_types:
            result_all  = merge_completion(alloc, compl, fetch_fn=fetch_fn)
            subj_all_df = _build_subject_agg(result_all)
            del result_all
        else:
            subj_all_df = pd.DataFrame()
    else:
        compl       = pd.DataFrame()
        result      = pd.DataFrame()
        subj_all_df = pd.DataFrame()

    # ── No-allocation stubs ───────────────────────────────────────────────────
    result_uids  = set(result["user_id"].dropna().unique()) if not result.empty else set()
    no_alloc_ids = set(chunk_ids) - result_uids

    if no_alloc_ids:
        stub = users_df[users_df["user_id"].isin(no_alloc_ids)][no_alloc_keep].copy()
        for col, val in [
            ("total_allocated", 0), ("total_lessons_allocated", 0),
            ("total_assessments_allocated", 0), ("total_completed", 0),
            ("total_lessons_completed", 0), ("total_assessments_completed", 0),
            ("completion_pct", 0.0), ("completed", 0),
        ]:
            stub[col] = val
        result = pd.concat([result, stub], ignore_index=True, sort=False) if not result.empty else stub

    return {
        "result":      result,
        "subj_all_df": subj_all_df,
        "alloc_raw":   alloc,        # pre-filter, for debug CSV
        "alloc_filt":  alloc_filtered,
        "no_alloc_ids": no_alloc_ids,
    }


def _process_chunks(
    all_chunks, n_chunks, start_chunk, users_df,
    cache, tbl, fetch_fn, alloc_precomputed, all_completion_df,
    centre_id, batch_id, subject_id, trade_id,
    output, active, all_lesson_types, dry_run, since,
    result_buf, is_small_run, user_id, subject_id_filter, trade_id_filter,
    workers: int,
):
    """
    Stage 3 — process all chunks (parallel when workers > 1).

    Returns (summary_rows, no_alloc_rows, first_write, first_all_write).
    """
    _alloc_from_cache  = (
        cache is not None and not alloc_precomputed and cache.is_ready()
    )
    _cache_total_rows  = [0]                 # mutable ref shared across workers
    _cache_lock        = threading.Lock()    # guards cache.append() + buffer.append()
    _buf_lock          = threading.Lock()    # guards result_buf.append()

    no_alloc_keep = [c for c in [
        "user_id", "user_name", "user_type", "is_master_trainer",
        "centre_id", "project_id", "is_ple", "batch_id", "trade_id",
    ] if c in users_df.columns]

    summary_rows       = []
    no_alloc_rows      = []
    first_write        = (start_chunk == 1)
    first_all_write    = (start_chunk == 1)

    def _write_chunk_result(chunk_idx, result, subj_all_df, alloc_raw, alloc_filt, no_alloc_ids):
        """Write one processed chunk's outputs. Called inside the worker."""
        nonlocal first_write, first_all_write

        if result.empty:
            return

        per_user_cols = [c for c in ["user_id", "allocation_path", "completion_pct"]
                         if c in result.columns]
        with _buf_lock:
            summary_rows.append(result.drop_duplicates("user_id")[per_user_cols].copy())
            if no_alloc_ids:
                no_alloc_rows.append(
                    users_df[users_df["user_id"].isin(no_alloc_ids)][no_alloc_keep].copy()
                )

        if n_chunks == 1:
            _print_summary(result)

        if dry_run:
            return

        subject_agg_df = _build_subject_agg(result)

        if output in ("db", "both"):
            if since:
                chunk_ids_list = result["user_id"].dropna().unique().tolist()
                delete_user_rows(ANALYTICS_DB, OUTPUT_TABLE_LESSON,  chunk_ids_list)
                delete_user_rows(ANALYTICS_DB, OUTPUT_TABLE_SUBJECT, chunk_ids_list)
                if all_lesson_types:
                    delete_user_rows(ANALYTICS_DB, OUTPUT_TABLE_SUBJECT_ALL, chunk_ids_list)
                db_mode     = "append"
                db_mode_all = "append"
            else:
                db_mode     = "replace" if first_write     else "append"
                db_mode_all = "replace" if first_all_write else "append"

        if "lesson" in active:
            if output in ("csv", "both"):
                _save_csv(result, user_id, centre_id, batch_id,
                          subject_id_filter, trade_id_filter, prefix="lessons_filtered")
            if output in ("db", "both"):
                with _buf_lock:
                    if result_buf:
                        result_buf.append("lesson", result)
                        log.info("[result_buf] Chunk %d buffered → lesson (%d rows, total: %d)",
                                 chunk_idx, len(result), result_buf.row_count("lesson"))
                    else:
                        write_table(ANALYTICS_DB, result, OUTPUT_TABLE_LESSON, if_exists=db_mode)
                        log.info("DB written → %s (chunk %d/%d, mode=%s)",
                                 OUTPUT_TABLE_LESSON, chunk_idx, n_chunks, db_mode)

        if "subject" in active:
            if output in ("csv", "both"):
                _save_csv(subject_agg_df, user_id, centre_id, batch_id,
                          subject_id_filter, trade_id_filter, prefix="subjects_filtered")
            if output in ("db", "both"):
                with _buf_lock:
                    if result_buf:
                        result_buf.append("subject", subject_agg_df)
                        log.info("[result_buf] Chunk %d buffered → subject (%d rows, total: %d)",
                                 chunk_idx, len(subject_agg_df), result_buf.row_count("subject"))
                    else:
                        write_table(ANALYTICS_DB, subject_agg_df, OUTPUT_TABLE_SUBJECT,
                                    if_exists=db_mode)
                        log.info("DB written → %s (chunk %d/%d, mode=%s)",
                                 OUTPUT_TABLE_SUBJECT, chunk_idx, n_chunks, db_mode)

            if all_lesson_types and output in ("db", "both") and not subj_all_df.empty:
                with _buf_lock:
                    if result_buf:
                        result_buf.append("subject_all", subj_all_df)
                    else:
                        write_table(ANALYTICS_DB, subj_all_df, OUTPUT_TABLE_SUBJECT_ALL,
                                    if_exists=db_mode_all)

        # Opt 10 — save checkpoint after successful write
        if cache is not None and not dry_run:
            with _cache_lock:
                cache.save_checkpoint(chunk_idx)

        first_write     = False
        first_all_write = False

    # ── Sequential path (workers=1) ───────────────────────────────────────────
    if workers <= 1:
        for chunk_idx, (chunk_ids, chunk_type) in enumerate(all_chunks, 1):
            if chunk_idx < start_chunk:
                if chunk_idx % 50 == 0 or chunk_idx == start_chunk - 1:
                    log.info("Skipping chunk %d / %d (already written)", chunk_idx, n_chunks)
                continue

            if n_chunks > 1:
                log.info("─── Chunk %d / %d  (%d %s users) ───",
                         chunk_idx, n_chunks, len(chunk_ids), chunk_type)

            out = _process_one_chunk(
                chunk_idx, chunk_ids, chunk_type, n_chunks, users_df,
                cache, tbl, fetch_fn, alloc_precomputed, all_completion_df,
                centre_id, batch_id, subject_id, trade_id,
                all_lesson_types, no_alloc_keep, _alloc_from_cache,
                _cache_lock, _cache_total_rows,
            )
            _write_chunk_result(
                chunk_idx,
                out["result"], out["subj_all_df"],
                out["alloc_raw"], out["alloc_filt"], out["no_alloc_ids"],
            )
            del out
            gc.collect()

    # ── Parallel path (workers > 1) ───────────────────────────────────────────
    else:
        log.info("Processing chunks with %d parallel workers", workers)

        chunks_to_run = [
            (chunk_idx, chunk_ids, chunk_type)
            for chunk_idx, (chunk_ids, chunk_type) in enumerate(all_chunks, 1)
            if chunk_idx >= start_chunk
        ]

        with ThreadPoolExecutor(max_workers=workers) as pool:
            future_to_idx = {
                pool.submit(
                    _process_one_chunk,
                    chunk_idx, chunk_ids, chunk_type, n_chunks, users_df,
                    cache, tbl, fetch_fn, alloc_precomputed, all_completion_df,
                    centre_id, batch_id, subject_id, trade_id,
                    all_lesson_types, no_alloc_keep, _alloc_from_cache,
                    _cache_lock, _cache_total_rows,
                ): chunk_idx
                for chunk_idx, chunk_ids, chunk_type in chunks_to_run
            }

            for future in as_completed(future_to_idx):
                chunk_idx = future_to_idx[future]
                try:
                    out = future.result()
                    if n_chunks > 1:
                        log.info("─── Chunk %d / %d done ───", chunk_idx, n_chunks)
                    _write_chunk_result(
                        chunk_idx,
                        out["result"], out["subj_all_df"],
                        out["alloc_raw"], out["alloc_filt"], out["no_alloc_ids"],
                    )
                    del out
                    gc.collect()
                except Exception as exc:
                    log.error("Chunk %d failed: %s", chunk_idx, exc, exc_info=True)
                    raise

    return summary_rows, no_alloc_rows, _cache_total_rows[0]


def _flush_outputs(result_buf, active, all_lesson_types, dry_run):
    """Stage 4 — flush all buffered results to analytics DB."""
    if result_buf is None or dry_run:
        return
    log.info("[result_buf] ── Flushing all buffered results to analytics DB ──────")
    if "lesson" in active:
        result_buf.flush("lesson", ANALYTICS_DB, OUTPUT_TABLE_LESSON, if_exists="replace")
    if "subject" in active:
        result_buf.flush("subject", ANALYTICS_DB, OUTPUT_TABLE_SUBJECT, if_exists="replace")
        if all_lesson_types:
            result_buf.flush("subject_all", ANALYTICS_DB, OUTPUT_TABLE_SUBJECT_ALL, if_exists="replace")
    log.info("[result_buf] ── Flush complete ────────────────────────────────────")


def _finalise(cache, tbl, alloc_precomputed, cache_total_rows):
    """Stage 5 — persist snapshot, clean up temp tables, close cache."""
    if cache is None:
        return
    if not alloc_precomputed and cache_total_rows > 0:
        cache.finalise(cache_total_rows)
        cache.save_snapshot()
        log.info("[cache] Allocation cache rebuilt — %d rows", cache_total_rows)
    if tbl is not None and tbl.alloc_precomputed_exists():
        tbl.drop_alloc_precomputed()
    cache.clear_checkpoint()
    cache.close()


# ─────────────────────────────────────────────────────────────────────────────
# Main entry point
# ─────────────────────────────────────────────────────────────────────────────

def run(
    user_id:          Optional[str] = None,
    centre_id:        Optional[str] = None,
    batch_id:         Optional[str] = None,
    subject_id:       Optional[str] = None,
    trade_id:         Optional[str] = None,
    since:            Optional[str] = None,
    output:           str = "db",
    dry_run:          bool = False,
    outputs:          str = "lesson,subject,debug",
    all_lesson_types: bool = False,
    start_chunk:      Optional[int] = None,
    force_refresh:    bool = False,
    workers:          Optional[int] = None,
) -> pd.DataFrame:

    active  = {o.strip().lower() for o in outputs.split(",")}
    n_workers = workers if workers is not None else CHUNK_WORKERS

    log.info("═" * 60)
    log.info(
        "AEL V2 pipeline starting  "
        "[user_id=%s | centre_id=%s | batch_id=%s | subject_id=%s | trade_id=%s"
        " | since=%s | output=%s | workers=%d | dry_run=%s]",
        user_id or "ALL", centre_id or "ALL", batch_id or "ALL",
        subject_id or "ALL", trade_id or "ALL",
        since or "NONE (full refresh)",
        output, n_workers, dry_run,
    )
    log.info("═" * 60)

    # ── Incremental scope ────────────────────────────────────────────────────
    changed_ids = None
    if since:
        ids = fetch_changed_user_ids(since)
        if not ids:
            log.info("Incremental run: no new completions since %s — nothing to update.", since)
            return pd.DataFrame()
        changed_ids = set(ids)
        log.info("Incremental run: %d users with new completions since %s", len(changed_ids), since)

    _scoped = any([user_id, centre_id, batch_id, subject_id, trade_id])

    # ── Stage 1: setup cache ─────────────────────────────────────────────────
    with TunnelPool() as pool:
        from config import SOURCE_DB
        pool.open(SOURCE_DB)
        pool.open(ANALYTICS_DB)

        cache, tbl, fetch_fn, alloc_precomputed, all_completion_df = _setup_cache(
            force_refresh=force_refresh,
            since=since,
            scoped=_scoped,
        )

        # ── Stage 2: fetch users ─────────────────────────────────────────────
        users_df = fetch_users(user_id, centre_id, batch_id, trade_id, fetch_fn=fetch_fn)
        if users_df.empty:
            log.warning("No users found — exiting.")
            if cache:
                cache.close()
            return pd.DataFrame()

        if changed_ids is not None:
            users_df = users_df[users_df["user_id"].isin(changed_ids)].reset_index(drop=True)
            if users_df.empty:
                log.info("None of the changed users match current filters — nothing to update.")
                if cache:
                    cache.close()
                return pd.DataFrame()
            log.info("Scoped to %d users matching filters and new-completion list", len(users_df))

        # ── Stage 2b: build chunks + resolve start_chunk ──────────────────────
        learner_chunks, staff_chunks, all_chunks = _build_chunks(users_df)
        n_chunks = len(all_chunks)
        is_small_run = (len(learner_chunks) <= 1 and len(staff_chunks) <= 1)

        # Opt 10 — auto-resume from checkpoint if start_chunk not given
        if start_chunk is None:
            if cache is not None:
                ckpt = cache.load_checkpoint()
                if ckpt and ckpt < n_chunks:
                    log.info(
                        "[checkpoint] Auto-resuming from chunk %d (last checkpoint: %d/%d)",
                        ckpt + 1, ckpt, n_chunks,
                    )
                    start_chunk = ckpt + 1
                else:
                    start_chunk = 1
            else:
                start_chunk = 1
        else:
            log.info("Manual resume from chunk %d", start_chunk)

        if start_chunk > 1:
            log.info("Resuming — chunks 1..%d already written; appending only.", start_chunk - 1)

        # ── Result buffer ────────────────────────────────────────────────────
        _use_result_buf = (
            cache is not None
            and not since
            and start_chunk == 1
            and output in ("db", "both")
        )
        result_buf = ResultBuffer(cache._con) if _use_result_buf else None
        if _use_result_buf:
            log.info("[result_buf] Result buffering ON — analytics DB writes deferred to end of run")

        # ── Stage 3: process chunks ──────────────────────────────────────────
        summary_rows, no_alloc_rows, cache_total_rows = _process_chunks(
            all_chunks=all_chunks,
            n_chunks=n_chunks,
            start_chunk=start_chunk,
            users_df=users_df,
            cache=cache,
            tbl=tbl,
            fetch_fn=fetch_fn,
            alloc_precomputed=alloc_precomputed,
            all_completion_df=all_completion_df,
            centre_id=centre_id,
            batch_id=batch_id,
            subject_id=subject_id,
            trade_id=trade_id,
            output=output,
            active=active,
            all_lesson_types=all_lesson_types,
            dry_run=dry_run,
            since=since,
            result_buf=result_buf,
            is_small_run=is_small_run,
            user_id=user_id,
            subject_id_filter=subject_id,
            trade_id_filter=trade_id,
            workers=n_workers,
        )

        # ── Stage 4: flush outputs ────────────────────────────────────────────
        _flush_outputs(result_buf, active, all_lesson_types, dry_run)

        # ── Save no-allocation user list ──────────────────────────────────────
        if no_alloc_rows and not dry_run and output in ("csv", "both"):
            no_alloc_keep_cols = [c for c in [
                "user_id", "user_name", "user_type", "is_master_trainer",
                "centre_id", "project_id", "is_ple", "batch_id", "trade_id",
            ] if c in users_df.columns]
            no_alloc_df = pd.concat(no_alloc_rows, ignore_index=True).drop_duplicates("user_id")
            os.makedirs(OUTPUT_DIR, exist_ok=True)
            ts   = datetime.now().strftime("%Y%m%d_%H%M%S")
            path = os.path.join(OUTPUT_DIR, f"no_allocation_users_{ts}.csv")
            no_alloc_df[no_alloc_keep_cols].to_csv(path, index=False)
            log.info("No-allocation users saved → %s (%d users)", path, len(no_alloc_df))

        # ── Stage 5: finalise ────────────────────────────────────────────────
        _finalise(cache, tbl, alloc_precomputed, cache_total_rows)

        # ── Final summary ─────────────────────────────────────────────────────
        if summary_rows:
            if n_chunks > 1:
                _print_summary_chunked(
                    pd.concat(summary_rows, ignore_index=True),
                    len(users_df),
                )
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

    df = run(
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
        workers=args.workers,
    )
    sys.exit(0 if not df.empty else 1)

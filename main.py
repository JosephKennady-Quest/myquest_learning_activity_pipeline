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
  total_allocated_lessons, total_completed_lessons, completion_pct,
  subj_total_allocated, subj_lessons_allocated, subj_assessments_allocated,
  subj_total_completed, subj_lessons_completed, subj_assessments_completed

Zero-completion users: one stub row per user with lesson/subject fields NULL,
completed = 0, total_completed_lessons = 0, completion_pct = 0.0.
"""

import argparse
import logging
import os
import sys
from datetime import datetime

import pandas as pd

from config import ANALYTICS_DB, ALLOC_CHUNK_SIZE, OUTPUT_DIR
from db import delete_user_rows, write_table
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

# Lesson-level detail table (one row per user × completed lesson)
OUTPUT_TABLE_LESSON   = "main_learning_activity_myquest_ael_lesson"
# Subject-level aggregation table (one row per user × subject) — primary output
OUTPUT_TABLE_SUBJECT  = "main_learning_activity_myquest_ael"

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
    return p.parse_args()


def _make_tag(
    user_id:    str | None,
    centre_id:  str | None = None,
    batch_id:   str | None = None,
    subject_id: str | None = None,
    trade_id:   str | None = None,
) -> str:
    """Build a short filename tag from whichever filters are active."""
    parts = []
    if user_id:
        parts.append(f"u{user_id[:8]}")
    if centre_id:
        parts.append(f"c{centre_id[:8]}")
    if batch_id:
        parts.append(f"b{batch_id[:8]}")
    if subject_id:
        parts.append(f"s{subject_id[:8]}")
    if trade_id:
        parts.append(f"t{trade_id[:8]}")
    return "_".join(parts) if parts else "all"


def _save_csv(
    df:         pd.DataFrame,
    user_id:    str | None,
    centre_id:  str | None = None,
    batch_id:   str | None = None,
    subject_id: str | None = None,
    trade_id:   str | None = None,
    prefix:     str = "allocation",
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
    prefix:     str = "allocation_debug",
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
    df = df[df["subject_id"].notna()] if "subject_id" in df.columns else df
    identity = [
        "user_id", "user_name", "user_type", "centre_id", "project_id",
        "batch_id", "trade_id", "career_path_id", "career_path_name",
        "subject_id", "subject_name", "subject_is_ple", "year_to_map", "allocation_basis",
        "total_allocated_lessons", "total_completed_lessons", "completion_pct",
        "subj_total_allocated", "subj_lessons_allocated", "subj_assessments_allocated",
        "subj_total_completed", "subj_lessons_completed", "subj_assessments_completed",
    ]
    first_cols = [c for c in identity if c in df.columns and c not in ("user_id", "subject_id")]
    agg = (
        df.groupby(["user_id", "subject_id"], as_index=False, sort=False)
        .agg(
            **{c: (c, "first") for c in first_cols},
            avg_score =("score",  "mean"),
            avg_rating=("rating", "mean"),
        )
    )
    agg["avg_score"]  = agg["avg_score"].round(2)
    agg["avg_rating"] = agg["avg_rating"].round(2)
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

    # ── Step 1: fetch users (lightweight — demographics only)
    users_df = fetch_users(user_id, centre_id, batch_id, trade_id)
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

    all_user_ids = users_df["user_id"].dropna().tolist()

    # ── Chunk the user list to avoid loading all allocation data at once.
    # Each chunk is processed end-to-end (allocation → completion → merge → write).
    # DB writes use "replace" on the first chunk and "append" on subsequent ones.
    chunks   = [all_user_ids[i:i + ALLOC_CHUNK_SIZE]
                for i in range(0, len(all_user_ids), ALLOC_CHUNK_SIZE)]
    n_chunks = len(chunks)

    if n_chunks > 1:
        log.info(
            "Large run: %d users → %d chunks of up to %d each",
            len(all_user_ids), n_chunks, ALLOC_CHUNK_SIZE,
        )

    first_write  = True           # replace on first chunk, append on the rest
    summary_rows = []             # accumulates one row per user for final report
    _no_alloc_keep = [c for c in ["user_id", "user_name", "user_type",
                                   "centre_id", "project_id", "batch_id", "trade_id"]
                      if c in users_df.columns]

    for chunk_idx, chunk_ids in enumerate(chunks, 1):
        if n_chunks > 1:
            log.info("─── Chunk %d / %d  (%d users) ───", chunk_idx, n_chunks, len(chunk_ids))

        # ── Step 2: allocation for this chunk ────────────────────────────────
        alloc = fetch_allocation(
            user_ids=chunk_ids,
            centre_id=centre_id, batch_id=batch_id,
            subject_id=subject_id, trade_id=trade_id,
        )
        alloc_filtered = _apply_lesson_type_filter(alloc) if not alloc.empty else alloc

        if not alloc.empty:
            log.info(
                "Lesson type filter: %d → %d rows (%d pdf/mp4/pdf-web excluded)",
                len(alloc), len(alloc_filtered), len(alloc) - len(alloc_filtered),
            )

        # Debug CSV — only for single-chunk runs (too large for full runs)
        if "debug" in active and not dry_run and n_chunks == 1 and not alloc_filtered.empty:
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
                    prefix="allocation_debug_all_types",
                )

        # ── Step 3: completion ────────────────────────────────────────────────
        if not alloc.empty:
            u_types = set(alloc["user_type"].dropna().astype(int).unique())
            a_ids   = alloc["user_id"].dropna().unique().tolist()
            log.info("Fetching completion for %d allocated users", len(a_ids))
            compl  = fetch_completion(user_ids=a_ids, user_types=u_types)
            result = merge_completion(alloc_filtered, compl)
        else:
            result = pd.DataFrame()

        # ── Add stubs for users in this chunk with no allocation ──────────────
        result_uids = set(result["user_id"].dropna().unique()) if not result.empty else set()
        no_alloc_ids = set(chunk_ids) - result_uids
        if no_alloc_ids:
            stub = (
                users_df[users_df["user_id"].isin(no_alloc_ids)]
                [_no_alloc_keep]
                .copy()
            )
            stub["total_allocated_lessons"] = 0
            stub["total_completed_lessons"] = 0
            stub["completion_pct"]          = 0.0
            stub["completed"]               = 0
            result = pd.concat([result, stub], ignore_index=True, sort=False) if not result.empty else stub
            log.info("Added %d users with no allocation (stub rows)", len(stub))

        if result.empty:
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
            continue

        # ── Write outputs ─────────────────────────────────────────────────────
        # Full refresh  → replace (TRUNCATE) on first chunk, append on the rest.
        # Incremental   → delete this chunk's user rows then append, so existing
        #                 rows for untouched users are never disturbed.
        subject_agg_df = _build_subject_agg(result)

        if output in ("db", "both"):
            if since:
                # Incremental: remove stale rows for this chunk's users, then insert
                delete_user_rows(ANALYTICS_DB, OUTPUT_TABLE_LESSON,  chunk_ids)
                delete_user_rows(ANALYTICS_DB, OUTPUT_TABLE_SUBJECT, chunk_ids)
                db_write_mode = "append"
            else:
                db_write_mode = "replace" if first_write else "append"

        if "lesson" in active:
            if output in ("csv", "both"):
                _save_csv(result, user_id, centre_id, batch_id, subject_id, trade_id)
            if output in ("db", "both"):
                write_table(ANALYTICS_DB, result, OUTPUT_TABLE_LESSON, if_exists=db_write_mode)
                log.info("DB written → %s  (chunk %d/%d, mode=%s)",
                         OUTPUT_TABLE_LESSON, chunk_idx, n_chunks, db_write_mode)

        if "subject" in active:
            if output in ("csv", "both"):
                _save_csv(subject_agg_df, user_id, centre_id, batch_id, subject_id, trade_id,
                          prefix="subject_agg")
            if output in ("db", "both"):
                write_table(ANALYTICS_DB, subject_agg_df, OUTPUT_TABLE_SUBJECT, if_exists=db_write_mode)
                log.info("DB written → %s  (chunk %d/%d, mode=%s)",
                         OUTPUT_TABLE_SUBJECT, chunk_idx, n_chunks, db_write_mode)

        # All-lesson-types variants (CSV only, single-chunk runs)
        if all_lesson_types and n_chunks == 1 and output in ("csv", "both"):
            result_all = merge_completion(alloc, compl)
            if "lesson" in active:
                _save_csv(result_all, user_id, centre_id, batch_id, subject_id, trade_id,
                          prefix="allocation_all_types")
            if "subject" in active:
                _save_csv(_build_subject_agg(result_all), user_id, centre_id, batch_id,
                          subject_id, trade_id, prefix="subject_agg_all_types")

        first_write = False

    # ── Final summary ─────────────────────────────────────────────────────────
    if summary_rows:
        if n_chunks > 1:
            _print_summary_chunked(pd.concat(summary_rows, ignore_index=True), len(all_user_ids))
    else:
        log.warning("No output rows produced — check DB connections and filters.")

    if dry_run:
        log.info("Dry run — no output written.")
    else:
        log.info("Pipeline complete.")

    return pd.DataFrame()


if __name__ == "__main__":
    args = parse_args()
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
    )
    sys.exit(0 if not df.empty else 1)

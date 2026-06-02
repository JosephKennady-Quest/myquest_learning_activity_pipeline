"""
Step 3 — Fetch lesson-level completion data and merge with allocation.

Source table routing (direct source DB — quest_rearch_production):

  User types 3, 4  (learners / alumni)
      → learning_activities

  All other user types  (facilitators, master trainers, etc.)
      → facilitator_learning_activities

Per (user_id, lesson_id) pair:
  - MAX(score)     → best score achieved
  - MAX(rating)    → best rating achieved
  - MAX(data_from) → last platform used (app / web)

A lesson is considered 'completed' only when a record with completed = 1
exists for that (user_id, lesson_id) pair. Records with completed != 1
(e.g. viewed-only or in-progress) are excluded at the SQL level.

merge_completion() LEFT JOINs onto the allocation DataFrame so that:
  - completed = 1  → user has activity recorded for this lesson
  - completed = 0  → lesson is allocated but not yet started
"""

import logging
from typing import List, Optional, Set

import pandas as pd

from config import LEARNER_TYPES, SOURCE_DB
from db import fetch

log = logging.getLogger(__name__)

_LEARNER_TYPES: Set[int] = set(LEARNER_TYPES)   # {3, 4}

# ── SQL template — table name is a safe internal constant, not user input ────
_SQL = """
SELECT
    user_id,
    lesson_id,
    MAX(score)      AS score,
    MAX(rating)     AS rating,
    NULL            AS data_from,
    SUM(duration)   AS duration

FROM `{table}`

WHERE completed = 1
  {user_clause}

GROUP BY user_id, lesson_id
"""


def _fetch_from(table: str, user_ids: List[str], fetch_fn=None) -> pd.DataFrame:
    """Run the completion query for a specific list of user_ids."""
    if not user_ids:
        return pd.DataFrame(columns=["user_id", "lesson_id", "score", "rating", "data_from"])
    placeholders = ", ".join(["%s"] * len(user_ids))
    sql = _SQL.format(table=table, user_clause=f"AND user_id IN ({placeholders})")
    return (fetch_fn or fetch)(SOURCE_DB, sql, tuple(user_ids))


def fetch_student_completion(user_ids: List[str], fetch_fn=None) -> pd.DataFrame:
    """
    Completion for learners / alumni (user types 3, 4).
    Source: learning_activities
    """
    src = "DuckDB cache" if fetch_fn else "production"
    df  = _fetch_from("learning_activities", user_ids, fetch_fn=fetch_fn)
    log.info("[s3_completion] learning_activities → %d rows  [%s]", len(df), src)
    return df


def fetch_facilitator_completion(user_ids: List[str], fetch_fn=None) -> pd.DataFrame:
    """
    Completion for facilitators and all other non-learner user types.
    Source: facilitator_learning_activities
    """
    src = "DuckDB cache" if fetch_fn else "production"
    df  = _fetch_from("facilitator_learning_activities", user_ids, fetch_fn=fetch_fn)
    log.info("[s3_completion] facilitator_learning_activities → %d rows  [%s]", len(df), src)
    return df


def fetch_completion(
    user_ids:   List[str],
    user_types: Optional[Set[int]] = None,
    fetch_fn=None,
) -> pd.DataFrame:
    """
    Fetch completion records for the given user_ids, routing to the correct
    source table based on user_types present in the current run.

    Args:
        user_ids:   List of user UUID strings to fetch completion for.
                    Always required — never queries the full table unfiltered.
        user_types: Set of integer user type values being processed.
                    Defaults to LEARNER_TYPES {3, 4} when not provided.

    Returns:
        DataFrame with columns:
            user_id, lesson_id, score, rating, data_from
    """
    if user_types is None:
        user_types = _LEARNER_TYPES

    frames = []

    learner_types = user_types & _LEARNER_TYPES          # {3, 4} overlap
    other_types   = user_types - _LEARNER_TYPES          # facilitators etc.

    if learner_types:
        frames.append(fetch_student_completion(user_ids, fetch_fn=fetch_fn))

    if other_types:
        frames.append(fetch_facilitator_completion(user_ids, fetch_fn=fetch_fn))

    if not frames:
        log.warning("[s3_completion] no user_types matched — returning empty DataFrame")
        return pd.DataFrame(columns=["user_id", "lesson_id", "score", "rating", "data_from"])

    combined = pd.concat(frames, ignore_index=True)

    # de-duplicate in case a user_id appears in both tables (edge case)
    combined = (
        combined
        .sort_values("score", ascending=False, na_position="last")
        .drop_duplicates(subset=["user_id", "lesson_id"], keep="first")
        .reset_index(drop=True)
    )

    log.info("[s3_completion] total completion records → %d", len(combined))
    return combined


def merge_completion(allocation: pd.DataFrame, completion: pd.DataFrame, fetch_fn=None) -> pd.DataFrame:
    """
    LEFT JOIN completion data onto the allocation DataFrame.

    Automatically routes the completion fetch for any user_types present
    in the allocation that were not yet covered (i.e. if allocation contains
    facilitator rows, their completion is fetched from
    facilitator_learning_activities).

    Added columns after merge:
        score                   — best score (NULL if not attempted)
        rating                  — best rating (NULL if not attempted)
        data_from               — last platform: app / web (NULL if not attempted)
        completed               — 1 = activity exists, 0 = not yet attempted
        total_allocated — total lessons allocated to this user
        total_completed — lessons with at least one activity record
        completion_pct          — completed / allocated × 100, rounded to 2dp
    """
    if allocation.empty:
        log.warning("[s3_completion] allocation is empty — nothing to merge")
        return allocation

    # If completion was passed empty, re-fetch scoped to the users in allocation
    if completion.empty and "user_type" in allocation.columns:
        user_types = set(allocation["user_type"].dropna().astype(int).unique())
        user_ids   = allocation["user_id"].dropna().unique().tolist()
        log.info("[s3_completion] re-fetching completion for %d users, user_types=%s",
                 len(user_ids), user_types)
        completion = fetch_completion(user_ids=user_ids, user_types=user_types, fetch_fn=fetch_fn)

    completion_cols = completion[["user_id", "lesson_id", "score", "rating", "data_from", "duration"]].copy()
    completion_cols["_matched"] = 1

    merged = allocation.merge(completion_cols, on=["user_id", "lesson_id"], how="left")
    merged["completed"] = merged["_matched"].fillna(0).astype(int)
    merged.drop(columns=["_matched"], inplace=True)

    # per-user summary — totals + assessment/lesson split for allocated and completed
    _ia_u = pd.to_numeric(merged["is_assessment"], errors="coerce").fillna(0).astype(int)
    summary = (
        merged.assign(
            _ia          = _ia_u,
            _comp_lesson = merged["completed"] * (1 - _ia_u),
            _comp_assess = merged["completed"] * _ia_u,
        )
        .groupby("user_id", as_index=False)
        .agg(
            total_allocated            =("lesson_id",      "count"),
            total_lessons_allocated    =("_ia",             lambda x: (x == 0).sum()),
            total_assessments_allocated=("_ia",             lambda x: (x == 1).sum()),
            total_completed            =("completed",       "sum"),
            total_lessons_completed    =("_comp_lesson",    "sum"),
            total_assessments_completed=("_comp_assess",    "sum"),
        )
    )
    summary["completion_pct"] = (
        summary["total_completed"] / summary["total_allocated"] * 100
    ).round(2)

    merged = merged.merge(summary, on="user_id", how="left")

    # ── Subject-level allocation summary (full dataset, before completed filter) ──
    _ia = pd.to_numeric(merged["is_assessment"], errors="coerce").fillna(0).astype(int)
    subj_alloc = (
        merged.assign(_ia=_ia)
        .groupby(["user_id", "subject_id"], as_index=False)
        .agg(
            subj_total_allocated     =("lesson_id", "count"),
            subj_lessons_allocated   =("_ia",       lambda x: (x == 0).sum()),
            subj_assessments_allocated=("_ia",      lambda x: (x == 1).sum()),
        )
    )
    merged = merged.merge(subj_alloc, on=["user_id", "subject_id"], how="left")

    # Keep only completed lessons
    completed = merged[merged["completed"] == 1].reset_index(drop=True)

    # ── Subject-level completion summary ──────────────────────────────────────
    _ia_c = pd.to_numeric(completed["is_assessment"], errors="coerce").fillna(0).astype(int)
    subj_compl = (
        completed.assign(_ia=_ia_c)
        .groupby(["user_id", "subject_id"], as_index=False)
        .agg(
            subj_total_completed      =("lesson_id", "count"),
            subj_lessons_completed    =("_ia",       lambda x: (x == 0).sum()),
            subj_assessments_completed=("_ia",       lambda x: (x == 1).sum()),
        )
    )
    completed = completed.merge(subj_compl, on=["user_id", "subject_id"], how="left")
    for col in ["subj_total_completed", "subj_lessons_completed", "subj_assessments_completed"]:
        completed[col] = completed[col].fillna(0).astype(int)

    # ── Zero-completion stub rows (one per user × subject) ───────────────────
    # Users who are allocated lessons but have no completed = 1 records at all
    # get one row per allocated SUBJECT — subject context is filled in so the
    # output shows which subjects they were supposed to complete; all
    # lesson-level columns are NULL; subject completion counts are 0.
    allocated_ids = set(allocation["user_id"].dropna().unique())
    completed_ids = set(completed["user_id"].dropna().unique()) if not completed.empty else set()
    zero_ids = allocated_ids - completed_ids

    if zero_ids:
        # Subject-level columns that are safe to keep (already present in merged)
        subj_cols = [c for c in [
            "user_id", "user_name", "user_type", "centre_id", "project_id",
            "batch_id", "trade_id", "career_path_id", "career_path_name",
            "subject_id", "subject_name", "subject_is_ple", "ple_career_path_id",
            "year_to_map", "trade_duration", "subject_order",
            "allocation_path", "allocation_basis",
            "subj_total_allocated", "subj_lessons_allocated", "subj_assessments_allocated",
        ] if c in merged.columns]

        zero_rows = (
            merged[merged["user_id"].isin(zero_ids)]
            .drop_duplicates(subset=["user_id", "subject_id"], keep="first")
            [subj_cols]
            .copy()
        )
        zero_rows = zero_rows.merge(
            summary[["user_id",
                      "total_allocated", "total_lessons_allocated", "total_assessments_allocated",
                      "total_completed", "total_lessons_completed", "total_assessments_completed",
                      "completion_pct"]],
            on="user_id", how="left",
        )
        zero_rows["completed"] = 0
        for col in ["subj_total_completed", "subj_lessons_completed", "subj_assessments_completed"]:
            zero_rows[col] = 0
        completed = pd.concat([completed, zero_rows], ignore_index=True, sort=False)
        log.info(
            "[s3_completion] added %d zero-completion rows (%d users, one row per allocated subject)",
            len(zero_rows), len(zero_ids),
        )

    avg_pct = summary["completion_pct"].mean() if not summary.empty else 0
    log.info(
        "[s3_completion] merged → %d rows | output rows → %d "
        "(completed lessons + %d zero-completion subject stubs) | avg completion %.1f%%",
        len(merged), len(completed), len(zero_ids), avg_pct,
    )
    return completed

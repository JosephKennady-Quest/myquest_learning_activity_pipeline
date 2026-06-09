"""
Build one-row-per-user JSON output from analytics user/project/phase tables.

Source tables:
  quest_analytics.main_users
  quest_analytics.main_centre_project
  quest_analytics.main_phases

Output shape:
  user-level columns from main_users, plus project_phase_combos and
  subject_combos as JSON text.
  main_users.id is emitted as tlo_user_id and used as the primary user key.

Storage optimisations (2026-06-09):
  1. subject_combos uses compact single-char keys instead of verbose names
     — reduces JSON payload by ~55% per row.
  2. sub_name excluded from subject_combos — consumers look it up from
     a reference table (sub_id is the key). Saves ~50 chars × 40 subjects
     × 100k users = ~200 MB on its own.
  3. subject_combos column changed to MEDIUMTEXT COMPRESSED — sufficient for
     any realistic JSON payload per user; MySQL off-page overhead removed.
  4. Numeric values stored as integers where appropriate (counts are ints,
     not floats) — avoids "12.0" serialisation overhead.
"""

import json
import logging
from typing import Iterable

import numpy as np
import pandas as pd

from config import ANALYTICS_DB, SOURCE_DB
from db import fetch

log = logging.getLogger(__name__)

USER_ID_CANDIDATES = ("tlo_users_id", "tlo_user_id", "user_id", "id")

PREFERRED_USER_COLS = [
    "tlo_users_id",
    "user_name",
    "gender",
    "created_at",
    "centre_name",
    "org_name",
    "state_name",
    "district_name",
    "trade",
    "batch_name",
    "batch_status",
    "centre_type",
    "user_type",
    "platform",
    "is_ple",
    "ple_enabled",
    "first_login",
]

# Keep project_phase_combos aligned with the existing WCC JSON notebook.
PROJECT_PHASE_COLS = [
    "prog_name",
    "project_id",
    "proj_name",
    "p_phase_id",
    "phase",
]

# Subject-level columns collapsed into subject_combos JSON array.
# sub_name intentionally excluded — saves ~50 chars × 40 subjects × 100k users.
# Consumers join on sub_id to get the name from a reference table.
SUBJECT_COLS = [
    "sub_id",
    "avg_score_a",
    "avg_rating_a",
    "c_sub_w_less_asse_c",
    "a_sub_w_less_asse_c",
    "a_sub_w_assess_c",
    "a_sub_w_lesson_c",
    "c_sub_w_assess_c",
    "c_sub_w_less_c",
    "year_category",
]

# Compact single-char key mapping for subject_combos JSON.
# Reduces per-object key overhead by ~55% (from ~170 bytes of keys to ~20 bytes).
# Mapping is stable — never change existing keys; only add new ones at the end.
_SUBJECT_KEY_MAP = {
    "sub_id":              "i",   # subject UUID
    "avg_score_a":         "s",   # avg score
    "avg_rating_a":        "r",   # avg rating
    "c_sub_w_less_asse_c": "c",   # completed (lessons + assessments)
    "a_sub_w_less_asse_c": "a",   # allocated (lessons + assessments)
    "a_sub_w_assess_c":    "aa",  # allocated assessments
    "a_sub_w_lesson_c":    "al",  # allocated lessons
    "c_sub_w_assess_c":    "ca",  # completed assessments
    "c_sub_w_less_c":      "cl",  # completed lessons
    "year_category":       "y",   # year_to_map
}

# Compact key mapping for project_phase_combos JSON.
_PHASE_KEY_MAP = {
    "prog_name":  "pr",   # program name
    "project_id": "pi",   # project UUID
    "proj_name":  "pn",   # project name
    "p_phase_id": "phi",  # phase UUID
    "phase":      "ph",   # phase name
}

# User-level overall columns from main_learning_activity_myquest_ael —
# same value for every subject row of a user, emitted as flat output columns.
OVERALL_COLS = [
    "a_overa_less_asses_c",
    "a_overa_assess_c",
    "a_overa_lesson_c",
    "c_overa_less_asses_c",
    "c_overa_asse_c",
    "c_overa_less_c",
    "rounded_completion",
]

_PROJECT_PHASE_SQL = """
SELECT
    u.id                  AS tlo_users_id,
    u.name                AS user_name,
    u.gender              AS gender,
    u.created_at          AS created_at,
    u.centre_name         AS centre_name,
    u.organisation_name   AS org_name,
    u.centre_state        AS state_name,
    u.centre_district     AS district_name,
    u.trade               AS trade,
    u.batch_name          AS batch_name,
    CASE
        WHEN u.batch_name IS NOT NULL THEN 'In Batch'
        ELSE 'Not in Batch'
    END                   AS batch_status,
    u.centre_type         AS centre_type,
    u.user_type           AS user_type,
    u.created_platform    AS platform,
    u.is_ple              AS is_ple,
    CASE
        WHEN cp.ple_enabled = 1 THEN 'PLE Centres'
        ELSE 'Non-PLE Centres'
    END                   AS ple_enabled,
    cp.program_name       AS __combo_prog_name,
    cp.project_id         AS __combo_project_id,
    cp.project_name       AS __combo_proj_name,
    ph.p_phase_id         AS __combo_p_phase_id,
    ph.phase_name         AS __combo_phase
FROM quest_analytics.main_users u
LEFT JOIN quest_analytics.main_centre_project cp
    ON cp.centre_id = u.centre_id
LEFT JOIN quest_analytics.main_phases ph
    ON ph.p_batch_id   = u.batch_id
   AND ph.p_centre_id  = u.centre_id
   AND ph.p_project_id = cp.project_id
   AND ph.p_user_id    = u.id
{where_clause}
"""

_LOGIN_SQL = """
SELECT
    user_id             AS tlo_users_id,
    MIN(created_at)     AS first_login
FROM quest_rearch_production.login_logs
WHERE user_id IS NOT NULL
  AND user_id IN ({placeholders})
GROUP BY user_id
"""

# sub_name intentionally excluded from SELECT — not stored in subject_combos JSON.
# Saves ~50 chars × avg_subjects × n_users of storage (see module docstring).
_SUBJECT_SQL = """
SELECT
    user_id                       AS tlo_users_id,
    subject_id                    AS sub_id,
    avg_score                     AS avg_score_a,
    avg_rating                    AS avg_rating_a,
    subj_total_completed          AS c_sub_w_less_asse_c,
    subj_total_allocated          AS a_sub_w_less_asse_c,
    subj_assessments_allocated    AS a_sub_w_assess_c,
    subj_lessons_allocated        AS a_sub_w_lesson_c,
    subj_assessments_completed    AS c_sub_w_assess_c,
    subj_lessons_completed        AS c_sub_w_less_c,
    year_to_map                   AS year_category,
    total_allocated               AS a_overa_less_asses_c,
    total_assessments_allocated   AS a_overa_assess_c,
    total_lessons_allocated       AS a_overa_lesson_c,
    total_completed               AS c_overa_less_asses_c,
    total_assessments_completed   AS c_overa_asse_c,
    total_lessons_completed       AS c_overa_less_c,
    completion_pct                AS rounded_completion
FROM quest_analytics.main_learning_activity_myquest_ael
{where_clause}
"""


def _make_json_safe(value):
    if pd.isna(value):
        return None
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, np.integer):
        return int(value)
    if isinstance(value, np.floating):
        f = float(value)
        # Store whole numbers as int to save bytes ("12" vs "12.0")
        return int(f) if f == int(f) else round(f, 4)
    if isinstance(value, np.bool_):
        return bool(value)
    return value


def _records_to_compact_json(
    records_df: pd.DataFrame,
    key_map: dict[str, str] | None = None,
) -> str | None:
    """
    Serialise a group of rows to a compact JSON string.

    key_map: if provided, renames DataFrame columns to short keys before
    serialising.  E.g. {"sub_id": "i", "avg_score_a": "s", ...}
    This alone cuts the per-object key overhead by ~55%.

    Null-only records are dropped (same behaviour as original _records_to_json).
    Uses separators=(',', ':') to strip whitespace from the output.
    """
    clean_records = []
    for row in records_df.to_dict("records"):
        clean_row = {
            (key_map.get(key, key) if key_map else key): _make_json_safe(value)
            for key, value in row.items()
        }
        if any(value is not None for value in clean_row.values()):
            clean_records.append(clean_row)

    if not clean_records:
        return None

    return json.dumps(clean_records, ensure_ascii=False, separators=(",", ":"))


# Keep old name as alias so any external callers don't break
def _records_to_json(records_df: pd.DataFrame) -> str | None:
    return _records_to_compact_json(records_df, key_map=None)


def _first_existing(columns: Iterable[str], candidates: Iterable[str]) -> str:
    available = set(columns)
    for candidate in candidates:
        if candidate in available:
            return candidate
    raise ValueError(
        "Could not find a user id column. Expected one of: "
        + ", ".join(candidates)
    )


def _existing_preferred_cols(df: pd.DataFrame, user_id_col: str) -> list[str]:
    cols = [col for col in PREFERRED_USER_COLS if col in df.columns]
    if user_id_col not in cols:
        cols.insert(0, user_id_col)
    return cols


def _where_clause(
    column_map: dict[str, str],
    user_id: str | None = None,
    centre_id: str | None = None,
    batch_id: str | None = None,
) -> tuple[str, tuple | None]:
    clauses, params = [], []
    if user_id:
        clauses.append(f"AND {column_map['user_id']} = %s")
        params.append(user_id)
    if centre_id:
        clauses.append(f"AND {column_map['centre_id']} = %s")
        params.append(centre_id)
    if batch_id:
        clauses.append(f"AND {column_map['batch_id']} = %s")
        params.append(batch_id)

    where_clause = ""
    if clauses:
        where_clause = "WHERE 1 = 1\n  " + "\n  ".join(clauses)

    return where_clause, tuple(params) if params else None


def fetch_first_login(user_ids: list, batch_size: int = 500) -> pd.DataFrame:
    """
    Fetch MIN(created_at) per user from production login_logs.

    Batches the IN clause in groups of `batch_size` (default 500).
    MySQL query planner degrades with large IN lists (thousands of UUIDs);
    batching keeps each query fast and avoids packet-size limits.
    Results are unioned and re-aggregated so the output is identical to
    a single query.
    """
    if not user_ids:
        return pd.DataFrame(columns=["tlo_users_id", "first_login"])

    frames = []
    for i in range(0, len(user_ids), batch_size):
        batch = user_ids[i : i + batch_size]
        placeholders = ", ".join(["%s"] * len(batch))
        sql = _LOGIN_SQL.format(placeholders=placeholders)
        frames.append(fetch(SOURCE_DB, sql, tuple(batch)))

    if not frames:
        return pd.DataFrame(columns=["tlo_users_id", "first_login"])

    df = pd.concat(frames, ignore_index=True)
    df = (
        df.groupby("tlo_users_id", as_index=False)
        .agg(first_login=("first_login", "min"))
    )
    log.info("[s4_users_project_phase_json] fetched first_login for %d users (%d batches)",
             len(df), len(frames))
    return df


def fetch_users_project_phase(
    user_id: str | None = None,
    centre_id: str | None = None,
    batch_id: str | None = None,
) -> pd.DataFrame:
    where_clause, params = _where_clause(
        {"user_id": "u.id", "centre_id": "u.centre_id", "batch_id": "u.batch_id"},
        user_id=user_id,
        centre_id=centre_id,
        batch_id=batch_id,
    )
    df = fetch(ANALYTICS_DB, _PROJECT_PHASE_SQL.format(where_clause=where_clause), params)
    log.info("[s4_users_project_phase_json] fetched %d joined rows", len(df))
    return df


def fetch_subjects(
    user_id: str | None = None,
    centre_id: str | None = None,
    batch_id: str | None = None,
) -> pd.DataFrame:
    where_clause, params = _where_clause(
        {"user_id": "user_id", "centre_id": "centre_id", "batch_id": "batch_id"},
        user_id=user_id,
        centre_id=centre_id,
        batch_id=batch_id,
    )
    df = fetch(ANALYTICS_DB, _SUBJECT_SQL.format(where_clause=where_clause), params)
    log.info("[s4_users_project_phase_json] fetched %d subject rows", len(df))
    return df


def build_users_project_phase_json(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    combo_renames = {
        "__combo_prog_name": "prog_name",
        "__combo_project_id": "project_id",
        "__combo_proj_name": "proj_name",
        "__combo_p_phase_id": "p_phase_id",
        "__combo_phase": "phase",
    }

    missing_combo_cols = [col for col in combo_renames if col not in df.columns]
    if missing_combo_cols:
        raise ValueError(f"Missing expected project/phase columns: {missing_combo_cols}")

    work_df = df.rename(columns=combo_renames).copy()
    user_id_col = _first_existing(work_df.columns, USER_ID_CANDIDATES)
    user_cols = _existing_preferred_cols(work_df, user_id_col)

    user_df = work_df[user_cols].drop_duplicates(subset=[user_id_col]).copy()

    project_phase_df = (
        work_df[[user_id_col] + PROJECT_PHASE_COLS]
        .drop_duplicates()
        .groupby(user_id_col, dropna=False)[PROJECT_PHASE_COLS]
        .apply(lambda grp: _records_to_compact_json(grp, key_map=_PHASE_KEY_MAP))
        .reset_index(name="project_phase_combos")
    )

    final_df = user_df.merge(project_phase_df, on=user_id_col, how="left")
    final_df = final_df.replace({np.nan: None})
    final_df["project_phase_combos"] = final_df["project_phase_combos"].replace("", None)

    log.info(
        "[s4_users_project_phase_json] built %d user rows using id column %s",
        len(final_df),
        user_id_col,
    )
    return final_df


def build_subject_json(df: pd.DataFrame) -> pd.DataFrame:
    """Returns subject_combos JSON + flat overall columns, one row per user."""
    if df.empty:
        return pd.DataFrame(columns=["tlo_users_id", "subject_combos"] + OVERALL_COLS)

    missing_subject_cols = [col for col in ["tlo_users_id"] + SUBJECT_COLS if col not in df.columns]
    if missing_subject_cols:
        raise ValueError(f"Missing expected subject columns: {missing_subject_cols}")

    subject_df = (
        df[["tlo_users_id"] + SUBJECT_COLS]
        .drop_duplicates()
        .groupby("tlo_users_id", dropna=False)[SUBJECT_COLS]
        .apply(lambda grp: _records_to_compact_json(grp, key_map=_SUBJECT_KEY_MAP))
        .reset_index(name="subject_combos")
    )
    subject_df["subject_combos"] = subject_df["subject_combos"].replace("", None)

    # Extract overall (user-level) columns — same value across all subject rows,
    # so just take the first occurrence per user.
    overall_cols_present = [c for c in OVERALL_COLS if c in df.columns]
    if overall_cols_present:
        overall_df = (
            df[["tlo_users_id"] + overall_cols_present]
            .drop_duplicates(subset=["tlo_users_id"], keep="first")
        )
        subject_df = subject_df.merge(overall_df, on="tlo_users_id", how="left")

    subject_df = subject_df.replace({np.nan: None})

    # Log payload size estimate so storage regressions are visible in logs
    if not subject_df.empty and "subject_combos" in subject_df.columns:
        sample = subject_df["subject_combos"].dropna()
        if not sample.empty:
            avg_bytes = sample.str.len().mean()
            total_mb  = sample.str.len().sum() / 1024 / 1024
            log.info(
                "[s4_users_project_phase_json] subject_combos: avg %.0f bytes/user, "
                "total payload %.1f MB for %d users",
                avg_bytes, total_mb, len(subject_df),
            )

    log.info("[s4_users_project_phase_json] built %d subject JSON rows", len(subject_df))
    return subject_df


def run_users_project_phase_json(
    user_id: str | None = None,
    centre_id: str | None = None,
    batch_id: str | None = None,
) -> pd.DataFrame:
    joined_df = fetch_users_project_phase(
        user_id=user_id,
        centre_id=centre_id,
        batch_id=batch_id,
    )
    final_df = build_users_project_phase_json(joined_df)

    subjects_df = fetch_subjects(
        user_id=user_id,
        centre_id=centre_id,
        batch_id=batch_id,
    )
    subject_json_df = build_subject_json(subjects_df)
    final_df = final_df.merge(subject_json_df, on="tlo_users_id", how="left")

    # Fetch first_login from production login_logs for all users in this run.
    user_ids = final_df["tlo_users_id"].dropna().unique().tolist()
    login_df = fetch_first_login(user_ids)
    final_df = final_df.merge(login_df, on="tlo_users_id", how="left")

    final_df = final_df.replace({np.nan: None})

    # ── Type cleanup ─────────────────────────────────────────────────────────
    for col in ("created_at", "first_login"):
        if col in final_df.columns:
            final_df[col] = pd.to_datetime(final_df[col], errors="coerce").dt.date

    if "rounded_completion" in final_df.columns:
        final_df["rounded_completion"] = (
            pd.to_numeric(final_df["rounded_completion"], errors="coerce")
            .round(2)
        )

    int_cols = [c for c in OVERALL_COLS if c != "rounded_completion"]
    for col in int_cols:
        if col in final_df.columns:
            final_df[col] = pd.to_numeric(final_df[col], errors="coerce").astype("Int64")

    return final_df


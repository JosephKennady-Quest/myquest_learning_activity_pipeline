"""
Step 2 — Build subject + lesson allocation per user.

Three allocation paths, merged in fetch_allocation():

  NON-PLE learners (type 3/4, is_ple IS NULL or != 1):
    centre_subject              — always applied (base)
    ∩ batch_subject             — only if user has a batch_id
    ∩ subject_trade             — only if user has a trade_id
    Lessons: student_access = 1

  PLE learners (type 3/4, is_ple = 1):
    centre_subject              — always applied (base)
    ∩ subject_ple_career_path   — only if user has an active career path
    ∩ batch_subject             — only if user has a batch_id
    Lessons: student_access = 1

  Staff (type 1 Admin, type 2 Facilitator / Master Trainer):
    centre_subject only — no batch, trade, or career path filter
    Lessons:
      Admin (type 1)                      → all lessons in the centre
      Facilitator (type 2, not MT)        → facilitator_access = 1
      Master Trainer (type 2, is_MT = 1)  → mastertrainer_access = 1
    Completion stored in facilitator_learning_activities.

  For learner paths — batch/trade/career_path are optional:
    If the relevant data exists → intersection is applied.
    If missing → that filter is skipped (user still gets centre allocation).

  Subjects and Lessons:
    status = 1, deleted_at IS NULL

  toolkit_type is derived from access flags on the lessons row:
    student_access = 1   → 'student'
    facilitator_access   → 'facilitator'
    mastertrainer_access → 'master'
"""

import logging
from typing import List, Optional

import pandas as pd

from config import LEARNER_TYPES_SQL, SOURCE_DB
from db import fetch

log = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Shared lesson + subject columns — learner queries (t_trade in scope)
# ─────────────────────────────────────────────────────────────────────────────
_COMMON_SELECT = """
    s.id                        AS subject_id,
    s.name                      AS subject_name,
    s.is_ple                    AS subject_is_ple,
    s.ple_career_path_id,
    s.year_to_map,
    cs.`order`                  AS subject_order,
    l.id                        AS lesson_id,
    l.name                      AS lesson_name,
    l.lesson_order,
    lt.name                     AS lesson_type,
    CASE
        WHEN l.is_assessment = 1
          OR UPPER(l.name) LIKE '%%ASSESSMENT%%' THEN 1
        ELSE 0
    END                         AS is_assessment,
    CASE
        WHEN l.student_access        = 1 THEN 'student'
        WHEN l.facilitator_access    = 1 THEN 'facilitator'
        WHEN l.mastertrainer_access  = 1 THEN 'master'
        ELSE NULL
    END                         AS toolkit_type,
    t_trade.duration            AS trade_duration
"""

# Staff query has no trades join — trade_duration is always NULL
_COMMON_SELECT_STAFF = _COMMON_SELECT.replace(
    "t_trade.duration            AS trade_duration",
    "NULL                        AS trade_duration",
)

# ─────────────────────────────────────────────────────────────────────────────
# Lesson JOINs for learners (student_access = 1 required)
# ─────────────────────────────────────────────────────────────────────────────
_LESSON_JOINS = """
JOIN subjects s
    ON  s.id          = cs.subject_id
    AND s.status      = 1
    AND s.deleted_at  IS NULL
JOIN lessons l
    ON  l.subject_id         = s.id
    AND l.status             = 1
    AND l.deleted_at         IS NULL
    AND l.student_access     = 1
    AND l.lesson_category_id = 'd78bc322-568f-4110-8e24-02ea444d48b7'
LEFT JOIN lesson_types lt
    ON  lt.id = l.lesson_type_id
"""

# Lesson JOINs for staff — no access flag here; access filter is in WHERE
_LESSON_JOINS_STAFF = """
JOIN subjects s
    ON  s.id          = cs.subject_id
    AND s.status      = 1
    AND s.deleted_at  IS NULL
JOIN lessons l
    ON  l.subject_id         = s.id
    AND l.status             = 1
    AND l.deleted_at         IS NULL
    AND l.lesson_category_id = 'd78bc322-568f-4110-8e24-02ea444d48b7'
LEFT JOIN lesson_types lt
    ON  lt.id = l.lesson_type_id
"""

# ─────────────────────────────────────────────────────────────────────────────
# NON-PLE: centre_subject, optionally ∩ batch_subject, optionally ∩ subject_trade
# ─────────────────────────────────────────────────────────────────────────────
_NON_PLE_SQL = """
SELECT
    u.id                        AS user_id,
    u.name                      AS user_name,
    u.type                      AS user_type,
    u.centre_id,
    u.project_id,
    sd.batch_id,
    sd.trade_id,
    NULL                        AS career_path_id,
    NULL                        AS career_path_name,
    NULL                        AS career_path_updated_at,
    {common_select}

FROM users u

LEFT JOIN student_details sd
    ON  sd.user_id   = u.id

JOIN centre_subject cs
    ON  cs.centre_id = u.centre_id

LEFT JOIN batch_subject bs
    ON  sd.batch_id   IS NOT NULL
    AND bs.batch_id   = sd.batch_id
    AND bs.subject_id = cs.subject_id

LEFT JOIN subject_trade st
    ON  sd.trade_id   IS NOT NULL
    AND st.trade_id   = sd.trade_id
    AND st.subject_id = cs.subject_id

LEFT JOIN trades t_trade
    ON  t_trade.id    = sd.trade_id

{lesson_joins}

WHERE u.type        IN ({types})
  AND u.status      = 1
  AND u.deleted_at  IS NULL
  AND (u.is_ple     IS NULL OR u.is_ple != 1)
  AND s.is_ple      IN (0, 2)
  AND (sd.batch_id  IS NULL OR bs.subject_id IS NOT NULL)
  AND (sd.trade_id  IS NULL OR st.subject_id IS NOT NULL)
  AND (s.year_to_map IS NULL OR s.year_to_map = 0 OR t_trade.duration IS NULL OR s.year_to_map <= t_trade.duration)
  {user_clause}

ORDER BY u.id, cs.`order`, l.lesson_order
"""

# ─────────────────────────────────────────────────────────────────────────────
# PLE: centre_subject, optionally ∩ subject_ple_career_path, optionally ∩ batch_subject
# ─────────────────────────────────────────────────────────────────────────────
_PLE_SQL = """
SELECT
    u.id                        AS user_id,
    u.name                      AS user_name,
    u.type                      AS user_type,
    u.centre_id,
    u.project_id,
    sd.batch_id,
    NULL                        AS trade_id,
    pcp.id                      AS career_path_id,
    pcp.name                    AS career_path_name,
    pcpu.career_path_updated_at,
    {common_select}

FROM users u

LEFT JOIN student_details sd
    ON  sd.user_id  = u.id

LEFT JOIN (
    SELECT user_id, job_type_id,
           updated_at             AS career_path_updated_at,
           ROW_NUMBER() OVER (
               PARTITION BY user_id
               ORDER BY     updated_at DESC
           )                      AS rn
    FROM   ple_career_path_user
    WHERE  status     = 1
      AND  deleted_at IS NULL
) pcpu ON pcpu.user_id = u.id AND pcpu.rn = 1

LEFT JOIN ple_career_paths pcp
    ON  pcp.id         = pcpu.job_type_id
    AND pcp.deleted_at IS NULL

JOIN centre_subject cs
    ON  cs.centre_id    = u.centre_id

LEFT JOIN subject_ple_career_path spcp
    ON  pcp.id        IS NOT NULL
    AND spcp.ple_career_path_id = pcp.id
    AND spcp.subject_id          = cs.subject_id

LEFT JOIN batch_subject bs
    ON  sd.batch_id   IS NOT NULL
    AND bs.batch_id   = sd.batch_id
    AND bs.subject_id = cs.subject_id

LEFT JOIN trades t_trade
    ON  t_trade.id    = sd.trade_id

{lesson_joins}

WHERE u.type        IN ({types})
  AND u.status      = 1
  AND u.deleted_at  IS NULL
  AND u.is_ple      = 1
  AND s.is_ple      IN (1, 2)
  AND (pcp.id       IS NULL OR spcp.subject_id IS NOT NULL)
  AND (sd.batch_id  IS NULL OR bs.subject_id   IS NOT NULL)
  AND (s.year_to_map IS NULL OR s.year_to_map = 0 OR t_trade.duration IS NULL OR s.year_to_map <= t_trade.duration)
  {user_clause}

ORDER BY u.id, pcpu.career_path_updated_at DESC, cs.`order`, l.lesson_order
"""

# ─────────────────────────────────────────────────────────────────────────────
# STAFF (types 1, 2): centre_subject only
#   Admin (type 1)           → all lessons
#   Facilitator (type 2)     → facilitator_access = 1
#   Master Trainer (type 2,
#     is_master_trainer = 1) → mastertrainer_access = 1
# ─────────────────────────────────────────────────────────────────────────────
_STAFF_SQL = """
SELECT
    u.id                        AS user_id,
    u.name                      AS user_name,
    u.type                      AS user_type,
    u.centre_id,
    u.project_id,
    NULL                        AS batch_id,
    NULL                        AS trade_id,
    NULL                        AS career_path_id,
    NULL                        AS career_path_name,
    NULL                        AS career_path_updated_at,
    {common_select}

FROM users u

JOIN centre_subject cs
    ON  cs.centre_id = u.centre_id

{lesson_joins}

WHERE u.type        IN (1, 2)
  AND u.status      = 1
  AND u.deleted_at  IS NULL
  AND s.is_ple      IN (0, 1, 2)
  AND (
      u.type = 1
      OR (u.type = 2 AND (u.is_master_trainer IS NULL OR u.is_master_trainer != 1) AND l.facilitator_access    = 1)
      OR (u.type = 2 AND u.is_master_trainer  = 1                                  AND l.mastertrainer_access  = 1)
  )
  {user_clause}

ORDER BY u.id, cs.`order`, l.lesson_order
"""


def _concat(frames: list) -> pd.DataFrame:
    """
    Concat non-empty DataFrames, casting all-NA columns to object dtype first.

    Columns that are NULL in one path but populated in another (e.g.
    career_path_id in non_ple, batch_id in staff) trigger a FutureWarning
    in pandas if their dtype is inferred from the all-NA frame. Casting to
    object before concat makes the dtype explicit and silences the warning.
    """
    frames = [f for f in frames if not f.empty]
    if not frames:
        return pd.DataFrame()
    if len(frames) == 1:
        return frames[0].reset_index(drop=True)
    fixed = []
    for df in frames:
        df = df.copy()
        for col in df.columns:
            if df[col].isna().all():
                df[col] = df[col].astype(object)
        fixed.append(df)
    return pd.concat(fixed, ignore_index=True)


def _build(
    template:   str,
    user_id:    Optional[str]       = None,
    user_ids:   Optional[List[str]] = None,
    centre_id:  Optional[str]       = None,
    batch_id:   Optional[str]       = None,
    subject_id: Optional[str]       = None,
    trade_id:   Optional[str]       = None,
) -> tuple:
    clauses, params = [], []
    if user_id:
        clauses.append("AND u.id          = %s")
        params.append(user_id)
    elif user_ids:
        ph = ", ".join(["%s"] * len(user_ids))
        clauses.append(f"AND u.id          IN ({ph})")
        params.extend(user_ids)
    if centre_id:
        clauses.append("AND u.centre_id   = %s")
        params.append(centre_id)
    if batch_id:
        clauses.append("AND sd.batch_id   = %s")
        params.append(batch_id)
    if subject_id:
        clauses.append("AND s.id          = %s")
        params.append(subject_id)
    if trade_id:
        clauses.append("AND sd.trade_id   = %s")
        params.append(trade_id)

    sql = template.format(
        common_select=_COMMON_SELECT,
        lesson_joins=_LESSON_JOINS,
        types=LEARNER_TYPES_SQL,
        user_clause="\n  ".join(clauses),
    )
    return sql, tuple(params) if params else None


def _build_staff(
    user_id:    Optional[str]       = None,
    user_ids:   Optional[List[str]] = None,
    centre_id:  Optional[str]       = None,
    subject_id: Optional[str]       = None,
) -> tuple:
    """Build SQL + params for staff query (no batch/trade filters — not applicable)."""
    clauses, params = [], []
    if user_id:
        clauses.append("AND u.id          = %s")
        params.append(user_id)
    elif user_ids:
        ph = ", ".join(["%s"] * len(user_ids))
        clauses.append(f"AND u.id          IN ({ph})")
        params.extend(user_ids)
    if centre_id:
        clauses.append("AND u.centre_id   = %s")
        params.append(centre_id)
    if subject_id:
        clauses.append("AND s.id          = %s")
        params.append(subject_id)

    sql = _STAFF_SQL.format(
        common_select=_COMMON_SELECT_STAFF,
        lesson_joins=_LESSON_JOINS_STAFF,
        user_clause="\n  ".join(clauses),
    )
    return sql, tuple(params) if params else None


def fetch_non_ple_allocation(
    user_id:    Optional[str]       = None,
    user_ids:   Optional[List[str]] = None,
    centre_id:  Optional[str]       = None,
    batch_id:   Optional[str]       = None,
    subject_id: Optional[str]       = None,
    trade_id:   Optional[str]       = None,
) -> pd.DataFrame:
    """
    Subjects allocated to non-PLE learners (types 3, 4).
    centre_subject is always the base; batch_subject and subject_trade
    intersections are applied only when the user has a batch_id / trade_id.
    """
    sql, params = _build(_NON_PLE_SQL, user_id, user_ids, centre_id, batch_id, subject_id, trade_id)
    df = fetch(SOURCE_DB, sql, params)
    log.info("[s2_allocation] non-PLE → %d rows", len(df))
    return df


def fetch_ple_allocation(
    user_id:    Optional[str]       = None,
    user_ids:   Optional[List[str]] = None,
    centre_id:  Optional[str]       = None,
    batch_id:   Optional[str]       = None,
    subject_id: Optional[str]       = None,
    trade_id:   Optional[str]       = None,
) -> pd.DataFrame:
    """
    Subjects allocated to PLE learners (types 3, 4, is_ple = 1).
    centre_subject is always the base; subject_ple_career_path and
    batch_subject intersections are applied only when the user has an
    active career path / batch_id respectively.
    When a career path exists, only the most recently updated active one
    is used (enforced via ROW_NUMBER() in the SQL).
    """
    sql, params = _build(_PLE_SQL, user_id, user_ids, centre_id, batch_id, subject_id, trade_id)
    df = fetch(SOURCE_DB, sql, params)
    log.info("[s2_allocation] PLE → %d rows", len(df))
    return df


def fetch_staff_allocation(
    user_id:    Optional[str]       = None,
    user_ids:   Optional[List[str]] = None,
    centre_id:  Optional[str]       = None,
    subject_id: Optional[str]       = None,
) -> pd.DataFrame:
    """
    Subjects allocated to staff (types 1 Admin, 2 Facilitator/Master Trainer).
    Allocation is centre_subject only — no batch, trade, or career path filter.
    Lesson access is filtered per user type in SQL:
      Admin           → all lessons
      Facilitator     → facilitator_access = 1
      Master Trainer  → mastertrainer_access = 1
    """
    sql, params = _build_staff(user_id, user_ids, centre_id, subject_id)
    df = fetch(SOURCE_DB, sql, params)
    log.info("[s2_allocation] staff → %d rows", len(df))
    return df


def fetch_allocation(
    user_id:    Optional[str]       = None,
    user_ids:   Optional[List[str]] = None,
    centre_id:  Optional[str]       = None,
    batch_id:   Optional[str]       = None,
    subject_id: Optional[str]       = None,
    trade_id:   Optional[str]       = None,
    paths:      tuple               = ("non_ple", "ple", "staff"),
) -> pd.DataFrame:
    """
    Combined allocation for users, tagged with 'allocation_path' for traceability.

    paths: restrict which allocation paths are executed — useful to avoid
    running irrelevant queries when the caller already knows the chunk
    contains only learners ("non_ple","ple") or only staff ("staff").
    Default is all three paths.
    """
    frames = []

    if "non_ple" in paths:
        non_ple = fetch_non_ple_allocation(user_id, user_ids, centre_id, batch_id, subject_id, trade_id)
        non_ple["allocation_path"]  = "non_ple"
        non_ple["allocation_basis"] = "centre_subject [→ batch_subject if batch] [→ subject_trade if trade]"
        frames.append(non_ple)

    if "ple" in paths:
        ple = fetch_ple_allocation(user_id, user_ids, centre_id, batch_id, subject_id, trade_id)
        ple["allocation_path"]      = "ple"
        ple["allocation_basis"]     = "centre_subject [→ subject_ple_career_path if career_path] [→ batch_subject if batch]"
        frames.append(ple)

    if "staff" in paths:
        staff = fetch_staff_allocation(user_id, user_ids, centre_id, subject_id)
        staff["allocation_path"]    = "staff"
        staff["allocation_basis"]   = "centre_subject (admin: all; facilitator: facilitator_access; master_trainer: mastertrainer_access)"
        frames.append(staff)

    combined = _concat(frames)

    if combined.empty:
        log.info("[s2_allocation] combined → 0 rows (no allocation found for this filter)")
        return combined

    before = len(combined)

    # A user enrolled in multiple career paths will produce one row per path
    # for every shared lesson. Keep the most recently updated career path
    # (career_path_updated_at DESC) — one unique row per (user_id, lesson_id).
    combined = (
        combined
        .sort_values(
            ["user_id", "career_path_updated_at", "lesson_order"],
            ascending=[True, False, True],
            na_position="last",
        )
        .drop_duplicates(subset=["user_id", "lesson_id"], keep="first")
        .reset_index(drop=True)
    )
    combined.drop(columns=["career_path_updated_at"], inplace=True)

    dropped = before - len(combined)
    if dropped:
        log.info(
            "[s2_allocation] deduplicated %d duplicate (user_id, lesson_id) rows "
            "(user enrolled in multiple career paths / subjects)",
            dropped,
        )

    log.info("[s2_allocation] combined → %d rows  paths=%s", len(combined), paths)
    return combined

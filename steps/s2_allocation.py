"""
Step 2 — Build subject + lesson allocation per user.

Allocation is built from centre_subject as the base, then additional
intersections are applied ONLY if the user has the corresponding data.
batch_id and trade_id (non-PLE) / career_path and batch_id (PLE) are
all optional — missing data means that filter is skipped, not that the
user gets zero allocation.

  NON-PLE users (is_ple IS NULL or != 1):
    centre_subject              — always applied (base)
    ∩ batch_subject             — only if user has a batch_id
    ∩ subject_trade             — only if user has a trade_id

    Examples:
      centre + batch + trade  → 3-way intersection
      centre + batch only     → 2-way intersection (trade skipped)
      centre + trade only     → 2-way intersection (batch skipped)
      centre only             → centre_subject allocation only

  PLE users (is_ple = 1):
    centre_subject              — always applied (base)
    ∩ subject_ple_career_path   — only if user has an active career path
    ∩ batch_subject             — only if user has a batch_id

    Examples:
      centre + career_path + batch → 3-way intersection
      centre + career_path only    → 2-way intersection (batch skipped)
      centre + batch only          → 2-way intersection (career path skipped)
      centre only                  → centre_subject allocation only

  Subjects and Lessons:
    status = 1, deleted_at IS NULL

  toolkit_type is derived from access flags on the lessons row:
    student_access = 1  → 'student'
    facilitator_access  → 'facilitator'
    mastertrainer_access → 'master'

  Only lessons with student_access = 1 are included (user types 3, 4).
"""

import logging
from typing import List, Optional

import pandas as pd

from config import LEARNER_TYPES_SQL, SOURCE_DB
from db import fetch

log = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Shared lesson + subject columns used in both queries
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

# ─────────────────────────────────────────────────────────────────────────────
# NON-PLE: centre_subject, optionally ∩ batch_subject, optionally ∩ subject_trade
# Each intersection is applied only when the user has the corresponding data.
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
    ON  bs.batch_id   = sd.batch_id
    AND bs.subject_id = cs.subject_id

LEFT JOIN subject_trade st
    ON  st.trade_id   = sd.trade_id
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
# Each intersection is applied only when the user has the corresponding data.
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
    ON  spcp.ple_career_path_id = pcp.id
    AND spcp.subject_id          = cs.subject_id

LEFT JOIN batch_subject bs
    ON  bs.batch_id   = sd.batch_id
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


def fetch_non_ple_allocation(
    user_id:    Optional[str]       = None,
    user_ids:   Optional[List[str]] = None,
    centre_id:  Optional[str]       = None,
    batch_id:   Optional[str]       = None,
    subject_id: Optional[str]       = None,
    trade_id:   Optional[str]       = None,
) -> pd.DataFrame:
    """
    Subjects allocated to non-PLE users.
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
    Subjects allocated to PLE users.
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


def fetch_allocation(
    user_id:    Optional[str]       = None,
    user_ids:   Optional[List[str]] = None,
    centre_id:  Optional[str]       = None,
    batch_id:   Optional[str]       = None,
    subject_id: Optional[str]       = None,
    trade_id:   Optional[str]       = None,
) -> pd.DataFrame:
    """
    Combined allocation for all users (PLE + non-PLE), tagged with
    an 'allocation_path' column for traceability.
    """
    non_ple = fetch_non_ple_allocation(user_id, user_ids, centre_id, batch_id, subject_id, trade_id)
    ple     = fetch_ple_allocation(user_id, user_ids, centre_id, batch_id, subject_id, trade_id)

    non_ple["allocation_path"]  = "non_ple"
    non_ple["allocation_basis"] = "centre_subject [→ batch_subject if batch] [→ subject_trade if trade]"

    ple["allocation_path"]      = "ple"
    ple["allocation_basis"]     = "centre_subject [→ subject_ple_career_path if career_path] [→ batch_subject if batch]"

    parts    = [df for df in [non_ple, ple] if not df.empty]
    combined = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()

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

    log.info("[s2_allocation] combined → %d rows (%d non-PLE + %d PLE)",
             len(combined), len(non_ple), len(ple))
    return combined

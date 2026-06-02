"""
Step 1 — Fetch users (all active types) with their profile details.

Filters applied:
  • users.type IN (1, 2, 3, 4)  — Admin, Facilitator/Master Trainer, Learner, Alumni
  • users.status = 1             — active accounts
  • users.deleted_at IS NULL

student_details is LEFT JOINed — staff users (types 1, 2) have no row there,
so batch_id / trade_id will be NULL for them.
"""

import logging
from typing import Optional

import pandas as pd

from config import ALL_TYPES_SQL, ANALYTICS_DB, SOURCE_DB
from db import fetch

log = logging.getLogger(__name__)

_SQL = """
SELECT
    u.id                                AS user_id,
    u.name                              AS user_name,
    u.email,
    u.mobile,
    u.type                              AS user_type,
    u.is_master_trainer,
    u.centre_id,
    u.project_id,
    u.organisation_id,
    u.is_ple,
    u.created_at,
    sd.batch_id,
    sd.trade_id,
    sd.educational_qualification_id,
    sd.placement_status_id,
    sd.active_year,
    sd.year_of_admission

FROM users u
LEFT JOIN student_details sd
    ON  sd.user_id = u.id

WHERE u.type        IN ({types})
  AND u.status      = 1
  AND u.deleted_at  IS NULL
  {user_clause}

ORDER BY u.id
"""


def fetch_users(
    user_id:   Optional[str] = None,
    centre_id: Optional[str] = None,
    batch_id:  Optional[str] = None,
    trade_id:  Optional[str] = None,
    fetch_fn=None,
) -> pd.DataFrame:
    """
    Return all active users (types 1–4) with their profile.
    Staff users (types 1, 2) will have NULL for student_details columns.
    Optionally filter by user_id, centre_id, batch_id, and/or trade_id.
    """
    clauses, params = [], []
    if user_id:
        clauses.append("AND u.id         = %s")
        params.append(user_id)
    if centre_id:
        clauses.append("AND u.centre_id  = %s")
        params.append(centre_id)
    if batch_id:
        clauses.append("AND sd.batch_id  = %s")
        params.append(batch_id)
    if trade_id:
        clauses.append("AND sd.trade_id  = %s")
        params.append(trade_id)

    sql = _SQL.format(types=ALL_TYPES_SQL, user_clause="\n  ".join(clauses))
    _fetch = fetch_fn or fetch
    src    = "DuckDB cache" if fetch_fn else "production"
    df     = _fetch(SOURCE_DB, sql, tuple(params) if params else None)
    log.info("[s1_users] fetched %d users (types 1–4) from %s", len(df), src)
    return df

"""
Step 1 — Fetch users (learners + alumni) with their student details.

Filters applied:
  • users.type IN (3, 4)   — learners and alumni only
  • users.status = 1       — active accounts
  • users.deleted_at IS NULL
"""

import logging
from typing import Optional

import pandas as pd

from config import ANALYTICS_DB, LEARNER_TYPES_SQL, SOURCE_DB
from db import fetch

log = logging.getLogger(__name__)

_SQL = """
SELECT
    u.id                                AS user_id,
    u.name                              AS user_name,
    u.email,
    u.mobile,
    u.type                              AS user_type,
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
) -> pd.DataFrame:
    """
    Return all active learners/alumni with their student profile.
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

    sql = _SQL.format(types=LEARNER_TYPES_SQL, user_clause="\n  ".join(clauses))
    df = fetch(SOURCE_DB, sql, tuple(params) if params else None)
    log.info("[s1_users] fetched %d users", len(df))
    return df

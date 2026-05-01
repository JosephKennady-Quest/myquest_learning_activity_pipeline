"""
Step 0 — Identify users with new completion activity since a given timestamp.

Used for incremental runs: only users who have at least one
  learning_activities.completed_at > since  AND  completed = 1
record are fetched, avoiding a full re-refresh when only a fraction of
users have new completions.

This step is skipped on full-refresh runs (--since not provided).
"""

import logging
from typing import List

from config import SOURCE_DB
from db import fetch

log = logging.getLogger(__name__)

_SQL = """
SELECT DISTINCT user_id
FROM   learning_activities
WHERE  completed    = 1
  AND  completed_at > %s
"""


def fetch_changed_user_ids(since: str) -> List[str]:
    """
    Return user_ids that have at least one new completed=1 record in
    learning_activities with completed_at after `since`.

    Args:
        since: datetime string, e.g. '2026-04-30 08:00:00'

    Returns:
        List of user_id UUID strings. Empty list means no new activity.
    """
    df = fetch(SOURCE_DB, _SQL, (since,))
    ids = df["user_id"].dropna().tolist()
    log.info(
        "[s0_changed_users] %d users with new completions since %s",
        len(ids), since,
    )
    return ids

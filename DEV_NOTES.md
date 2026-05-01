# AEL V2 Pipeline — Development Notes

> This file is a running log of all decisions, bug fixes, and business logic choices made during development. Read this at the start of every new session to pick up exactly where we left off.

---

## What This Pipeline Does (Quick Summary)

Replaces the original Java/Talend ETL. For every active learner/alumni (user types 3 & 4), it produces:
- One row per **completed lesson** showing what was allocated, whether it was completed, score, rating, and per-subject counts
- One **stub row per user** for users with zero completions (lesson fields NULL, `completed = 0`) — so every allocated user appears in the output
- A **subject-level aggregation** table (one row per user × subject) with avg score/rating and counts

Output: DB tables in `quest_ple_analytics` (default) and/or CSV files.

| DB Table | Contents |
|---|---|
| `main_learning_activity_myquest_ael` | Subject-level aggregation (primary analytics table) |
| `main_learning_activity_myquest_ael_lesson` | Lesson-level detail |

---

## Current Status — What Is Done

| File | Status | Notes |
|---|---|---|
| `config.py` | Done | Loads `.env`, builds `SOURCE_DB` + `ANALYTICS_DB` config dicts |
| `db.py` | Done | Custom SSH tunnel (paramiko, no sshtunnel library) + fetch/write helpers |
| `steps/s1_users.py` | Done | Fetches active users type 3 & 4; supports user_id, centre_id, batch_id, trade_id filters |
| `steps/s2_allocation.py` | Done | PLE + non-PLE allocation; supports user_id, centre_id, batch_id, subject_id, trade_id filters |
| `steps/s3_completion.py` | Done | Routes to correct source table; `WHERE completed = 1` in SQL; subject-level counts; zero-completion stub rows |
| `main.py` | Done | CLI: --user-id, --centre-id, --batch-id, --subject-id, --trade-id, --output db(default)/csv/both, --outputs, --all-lesson-types, --dry-run; writes to `main_learning_activity_myquest_ael` (subject) + `main_learning_activity_myquest_ael_lesson` |
| `README.md` | Done | Full technical documentation |
| `DEV_NOTES.md` | This file | Development log |

**Pipeline runs successfully.** Tested with a single PLE user (see Test User section below).

---

## Business Logic — Key Decisions

### User Types
- Only user types **3 (learner)** and **4 (alumni)** are processed
- `users.status = 1` and `users.deleted_at IS NULL`

### Two Allocation Paths

**Non-PLE** (`users.is_ple IS NULL or != 1`):
- Subject must appear in ALL THREE: `centre_subject` ∩ `batch_subject` ∩ `subject_trade`
- Requires `student_details.batch_id IS NOT NULL` AND `trade_id IS NOT NULL`

**PLE** (`users.is_ple = 1`):
- Subject must appear in ALL THREE: `centre_subject` ∩ `subject_ple_career_path` ∩ `batch_subject`
- Career path resolved via `ple_career_path_user` → `ple_career_paths` (status=1, deleted_at IS NULL)
- Requires `student_details.batch_id IS NOT NULL`

### Year-to-Map Filter (added in session)
- `subjects.year_to_map` restricts which year of a multi-year trade a subject belongs to
- Rule: include subject only if `subjects.year_to_map <= trades.duration`
- 1-year trade → only year_to_map=1 subjects
- 2-year trade → year_to_map=1 AND year_to_map=2 subjects
- Pass-through (always include): `year_to_map IS NULL`, `year_to_map = 0`, `trades.duration IS NULL`
- Trade joined via: `LEFT JOIN trades t_trade ON t_trade.id = sd.trade_id`

### subjects.is_ple Filter
- `subjects.is_ple` controls which platform/user-type the subject is for:
  - `0` = QuestApp — non-PLE users only
  - `1` = MyQuest — PLE users only
  - `2` = Both — all users
- Non-PLE query: `AND s.is_ple IN (0, 2)`
- PLE query: `AND s.is_ple IN (1, 2)`

### Completion Source Table Routing (added in session)
- User types 3, 4 → `learning_activities` (direct source DB: `quest_rearch_production`)
- All other types → `facilitator_learning_activities`
- `data_from` column does NOT exist in source tables → always `NULL AS data_from`
- **`completed = 1` filter in SQL**: The query includes `WHERE completed = 1` — only records explicitly marked completed are fetched. Viewed-only or in-progress records (`completed = 0` / NULL) are excluded at the DB level before merging.

### Completed = 1 Filter (two-layer enforcement)
- **Layer 1 — SQL**: `WHERE completed = 1` in the `_SQL` template in `s3_completion.py`. Only records explicitly marked `completed = 1` in `learning_activities` / `facilitator_learning_activities` are fetched. Viewed-only rows (`completed = 0` or NULL) never enter the pipeline.
- **Layer 2 — Python**: `merge_completion()` LEFT JOINs completion onto allocation. Allocated lessons with no `completed = 1` record get `completed = 0` and are dropped before returning.
- Summary stats (`total_allocated`, `total_completed`, `completion_pct`) are calculated on the **full** merged dataset BEFORE the Python-level filter.
- This prevents "extra" completions (outside allocation) because the LEFT JOIN is from the allocation side.

### Zero-Completion Users (stub rows)
- **Rule**: Every user who has been allocated lessons should appear in the output, even if they have completed nothing.
- **Implementation**: At the end of `merge_completion()`, after the `completed = 1` filter, the function finds users who are in the allocation but have no completed rows. For each such user, one stub row is added with: demographic columns (user_id, name, type, centre_id, etc.) filled, summary stats (`total_allocated`, all split counts, `total_completed = 0`, `completion_pct = 0.0`) filled, all lesson/subject columns (lesson_id, subject_id, score, etc.) as NULL, `completed = 0`.
- **Why**: This ensures dashboards and reports can count all allocated users, not just those who have started.
- **Note**: Zero-completion stub rows are included in the lesson-level output but excluded from the subject-level aggregation (no subject to aggregate on).

### Subject ID and Trade ID Filters
- **subject_id filter**: `AND s.id = %s` added to both PLE and non-PLE allocation queries. Scopes the entire pipeline to one subject.
- **trade_id filter**: `AND sd.trade_id = %s` in both queries + `AND sd.trade_id = %s` in `s1_users.py`. PLE users without a trade_id in `student_details` will return zero rows when this filter is active (no `sd.trade_id` to match against) — this is expected behaviour.
- Both filters are optional and can be freely combined with user_id, centre_id, batch_id.

### Default Output Mode Changed to DB
- `--output` default changed from `"csv"` to `"db"`.
- Rationale: production runs push to the analytics DB; CSV was a debugging convenience.
- Use `--output csv` or `--output both` explicitly when you want file output.

### is_assessment — Name-Based Detection
- `lessons.is_assessment` flag is not always set correctly in the source DB — some lessons are assessments by name but have `is_assessment = 0` or NULL.
- **Rule**: A lesson is treated as an assessment if `is_assessment = 1` **OR** `UPPER(name) LIKE '%%ASSESSMENT%%'`.
- **Implementation**: SQL `CASE WHEN l.is_assessment = 1 OR UPPER(l.name) LIKE '%%ASSESSMENT%%' THEN 1 ELSE 0 END AS is_assessment` in `_COMMON_SELECT` in `s2_allocation.py`. The `%%` escaping is required by pymysql — a single `%` in a SQL string is interpreted as a Python format specifier and causes a `ValueError`.
- All downstream counts (`subj_assessments_allocated`, `subj_assessments_completed`, `total_assessments_allocated`, `total_assessments_completed`) automatically use the corrected flag.

### duration Column from learning_activities
- `learning_activities.duration` holds the time (in seconds) a user spent on a lesson per attempt.
- **Lesson level**: `SUM(duration)` is fetched per `(user_id, lesson_id)` — total time across all completed attempts for that lesson.
- **Subject level**: `avg_duration` (mean per lesson) and `total_duration` (sum across all lessons) are computed in `_build_subject_agg()` in `main.py`.
- `duration` is fetched via `s3_completion.py` and passed through `merge_completion()` into the result DataFrame. The `completion_cols` selection in `merge_completion()` must explicitly include `"duration"` — forgetting this drops the column before the LEFT JOIN.

### User-Level Assessment / Lesson Split Columns
- **Renamed**: `total_allocated_lessons` → `total_allocated`, `total_completed_lessons` → `total_completed` for clarity.
- **New columns** (user-level, both tables):
  - `total_lessons_allocated` — non-assessment lessons allocated (user total)
  - `total_assessments_allocated` — assessment lessons allocated (user total)
  - `total_lessons_completed` — non-assessment lessons completed (user total)
  - `total_assessments_completed` — assessment lessons completed (user total)
- **Implementation**: Computed in the per-user `summary` groupby in `merge_completion()` using `_ia` (is_assessment flag), `_comp_lesson = completed * (1 - _ia)`, and `_comp_assess = completed * _ia` helper columns.
- Subject-level equivalents (`subj_lessons_allocated` etc.) were already present — these new columns are the user-level totals across all subjects.

### No-Allocation User CSV Report
- After every run (except dry run), if any users have completions but no current allocation, their details are saved to `output/no_alloc_users_<timestamp>.csv`.
- Columns: `user_id`, `user_name`, `user_type`, `centre_id`, `is_ple`, `batch_id`, `trade_id`.
- Accumulated across all chunks, deduplicated on `user_id`, written once at end of run.
- Use this to investigate data quality issues: missing batch/trade/career path mappings for active users.

### Compressed Log Rotation (--log-file)
- `--log-file <path>` adds a `TimedRotatingFileHandler` to the root logger alongside stdout.
- Rotates daily at midnight; rotated files are gzip-compressed via a custom `rotator` + `namer` on the handler.
- Active file: `<path>` (plain text). Rotated: `<path>.YYYY-MM-DD.gz`. Kept for 30 days (`backupCount=30`).
- Recommended for cron jobs so every run is permanently logged without consuming unlimited disk.

### PLE Career Path — Latest Active Only (final fix)
- **Rule**: Each PLE user must have exactly ONE career path: the most recently updated active one.
- **SQL fix**: `ple_career_path_user` is now wrapped in a derived table using `ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY updated_at DESC)` — only `rn = 1` row is joined. This enforces the constraint at the DB level, not in Python.
- **Python dedup** (safety net): `fetch_allocation()` still deduplicates on `(user_id, lesson_id)` sorted by `career_path_updated_at DESC` as a safety net for any edge cases (e.g. same timestamp on two records).
- Root cause: test user had 2 active career paths. Old approach joined both → 625 rows for 342 unique lessons. With the SQL fix, only the more recently updated career path is joined.

---

## Bugs Fixed (with Root Causes)

### 1. `sshtunnel` AttributeError — `paramiko.DSSKey`
- **Error:** `AttributeError: module 'paramiko' has no attribute 'DSSKey'`
- **Cause:** `sshtunnel` 0.4.0 uses `paramiko.DSSKey` which was removed in paramiko 3.x
- **Fix:** Completely removed `sshtunnel`. Replaced with custom local port forwarder in `db.py`:
  - `paramiko.SSHClient` connects to bastion
  - `socket.bind("127.0.0.1", 0)` picks a free local port
  - Background thread accepts TCP connections and bridges them to `direct-tcpip` channels
  - `pymysql.connect(host="127.0.0.1", port=local_port)` connects through the tunnel
- `requirements.txt` no longer includes `sshtunnel`

### 2. `pymysql` `sock=` TypeError
- **Error:** `TypeError: Connection.__init__() got an unexpected keyword argument 'sock'`
- **Cause:** Tried passing a paramiko channel as `sock=` directly to `pymysql.connect()` — not supported
- **Fix:** Same local port forwarder fix as above

### 3. Unknown column `data_from`
- **Error:** `OperationalError: (1054, "Unknown column 'data_from' in 'field list'")`
- **Cause:** `s3_completion.py` query had `MAX(data_from) AS data_from` but `learning_activities` table has no such column (it exists only in analytics staging tables)
- **Fix:** Replaced with `NULL AS data_from` in the SQL template

### 4. pandas SQLAlchemy warning
- **Error:** `UserWarning: pandas only supports SQLAlchemy connectable`
- **Cause:** `pd.read_sql()` called with raw pymysql connection
- **Fix:** Replaced with `cursor.execute()` + `pd.DataFrame(rows, columns=column_names)`

### 5. Duplicate lesson rows
- **Error:** 566 out of 625 rows were duplicates for the test user
- **Cause:** User enrolled in 2 career paths; PLE query joins to ALL active career paths, producing one row per career path per shared lesson
- **Fix:** `drop_duplicates(subset=["user_id", "lesson_id"])` in `fetch_allocation()` after combining

### 6. `ValueError: unsupported format character 'A'` on LIKE clause
- **Error:** `ValueError: unsupported format character 'A' (0x41) at index 861`
- **Cause:** `LIKE '%ASSESSMENT%'` in SQL — pymysql uses `%` as a Python format specifier in `mogrify()`, so `%A` is treated as an invalid format code
- **Fix:** Escape all literal `%` signs as `%%` in SQL strings: `LIKE '%%ASSESSMENT%%'`

### 7. `OperationalError: (1054, "Unknown column 'duration' in 'field list'")`
- **Error:** INSERT into `main_learning_activity_myquest_ael_lesson` failed with unknown column
- **Cause:** Table existed from a previous run with the old schema (no `duration` column). `TRUNCATE` keeps the schema, so the new `duration` column in the DataFrame had no matching column in the table.
- **Fix:** Changed `write_table()` in `db.py` to use `DROP TABLE IF EXISTS` + `CREATE TABLE` on full refresh instead of `TRUNCATE`. This always rebuilds the schema from the current DataFrame — any new columns are picked up automatically.

### 8. `duration` column missing from merge result
- **Error:** `KeyError: "Label(s) ['duration'] do not exist"` in `_build_subject_agg()`
- **Cause:** `merge_completion()` explicitly selected only `["user_id", "lesson_id", "score", "rating", "data_from"]` from the completion DataFrame before the LEFT JOIN — `duration` was dropped before merging
- **Fix:** Added `"duration"` to the `completion_cols` column list in `merge_completion()`

---

## File-by-File Reference

### `config.py`
```python
SOURCE_DB   = CONFIG["source"]       # quest_rearch_production
ANALYTICS_DB = CONFIG["destination"] # quest_ple_analytics
LEARNER_TYPES     = (3, 4)
LEARNER_TYPES_SQL = "3,4"
CHUNK_SIZE  = 5000
OUTPUT_DIR  = os.getenv("OUTPUT_DIR", "output")
DB_CONFIG_DIR = os.path.join(_PIPELINE_DIR, "DB_Config")  # .pem files live here
```

### `db.py`
- `fetch(cfg, sql, params)` → DataFrame
- `write_table(cfg, df, table, if_exists="replace")` → None
- `delete_user_rows(cfg, table, user_ids)` → None (incremental mode: removes stale rows before re-insert)
- **Full refresh write strategy**: `DROP TABLE IF EXISTS` + `CREATE TABLE` (from DataFrame dtypes) + `INSERT`. This replaces the old `TRUNCATE` approach so new columns in the DataFrame are always picked up — no manual `ALTER TABLE` needed.
- **Incremental write strategy**: `DELETE WHERE user_id IN (...)` + `INSERT` (append). Leaves untouched users' rows in place.
- Each call opens and closes its own SSH tunnel (no persistent pool)

### `steps/s1_users.py`
- `fetch_users(user_id=None, centre_id=None, batch_id=None, trade_id=None)` → DataFrame
- Source: `users` LEFT JOIN `student_details`
- Filter: `type IN (3,4)`, `status=1`, `deleted_at IS NULL`
- Dynamic WHERE clauses for all 4 optional params

### `steps/s2_allocation.py`
- `fetch_non_ple_allocation(user_id, user_ids, centre_id, batch_id, subject_id, trade_id)` → DataFrame
- `fetch_ple_allocation(user_id, user_ids, centre_id, batch_id, subject_id, trade_id)` → DataFrame
- `fetch_allocation(user_id, user_ids, centre_id, batch_id, subject_id, trade_id)` → DataFrame (combined, deduplicated)
- `subject_id` → `AND s.id = %s` in SQL (filters both PLE and non-PLE queries)
- `trade_id` → `AND sd.trade_id = %s` in SQL (PLE users without trade_id return 0 rows)
- `user_ids` → `AND u.id IN (...)` — used by chunked processing in `main.py`
- Adds `allocation_path` (`"ple"` or `"non_ple"`) and `allocation_basis` columns
- **`is_assessment`**: `CASE WHEN l.is_assessment = 1 OR UPPER(l.name) LIKE '%%ASSESSMENT%%' THEN 1 ELSE 0 END` — catches lessons where DB flag was not set

### `steps/s3_completion.py`
- `fetch_student_completion(user_ids)` → DataFrame (from `learning_activities`, `WHERE completed = 1`)
  - Fetches: `user_id`, `lesson_id`, `MAX(score)`, `MAX(rating)`, `NULL AS data_from`, `SUM(duration)`
- `fetch_facilitator_completion(user_ids)` → DataFrame (from `facilitator_learning_activities`, `WHERE completed = 1`)
- `fetch_completion(user_ids, user_types={3,4})` → DataFrame (auto-routes; always requires `user_ids` list)
- `merge_completion(allocation_df, completion_df)` → DataFrame
  - LEFT JOIN completion onto allocation (includes `duration` column)
  - Computes per-user summary: `total_allocated`, `total_lessons_allocated`, `total_assessments_allocated`, `total_completed`, `total_lessons_completed`, `total_assessments_completed`, `completion_pct`
  - Computes per-(user, subject) allocation counts: `subj_total_allocated`, `subj_lessons_allocated`, `subj_assessments_allocated`
  - Computes per-(user, subject) completion counts: `subj_total_completed`, `subj_lessons_completed`, `subj_assessments_completed`
  - Appends one stub row per zero-completion user (lesson/subject fields NULL, `completed = 0`, all completion counts = 0)
  - Returns completed rows + zero-completion stubs

### `main.py`
```bash
# All users → DB (default, full refresh)
python main.py

# Single user → CSV
python main.py --user-id <uuid> --output csv

# Incremental: only users with new completions since timestamp
python main.py --since "2026-04-30 00:00:00"

# All users in a centre / batch / trade / subject
python main.py --centre-id <uuid>
python main.py --batch-id <uuid>
python main.py --trade-id <uuid>
python main.py --subject-id <uuid>

# Combine any filters freely
python main.py --centre-id <uuid> --batch-id <uuid> --trade-id <uuid>

# Output modes
python main.py --output csv            # CSV only
python main.py --output db             # DB only (default)
python main.py --output both           # CSV + DB

# Control which outputs are written
python main.py --outputs subject       # subject aggregation only
python main.py --outputs lesson,subject # lesson detail + subject agg (no debug)

# Write compressed rotating log file (in addition to stdout)
python main.py --log-file /home/joseph/logs/ael_pipeline.log

# Dry run (no output written)
python main.py --dry-run
```
- CSV filename tag reflects active filters: `u<8>_c<8>_s<8>_t<8>` or `all`
- `OUTPUT_TABLE_SUBJECT  = "main_learning_activity_myquest_ael"` (subject agg — primary)
- `OUTPUT_TABLE_LESSON   = "main_learning_activity_myquest_ael_lesson"` (lesson detail)
- After every non-dry run: `output/no_alloc_users_<timestamp>.csv` if any users had completions but no allocation
- Log line: `[user_id=ALL | centre_id=... | batch_id=ALL | subject_id=ALL | trade_id=ALL | since=... | output=db | ...]`

---

## Infrastructure

### SSH Tunnel Details
| | Source DB | Analytics DB |
|---|---|---|
| Bastion IP | `SOURCE_SSH_HOST` (see `.env`) | `DEST_SSH_HOST` (see `.env`) |
| SSH User | `SOURCE_SSH_USER` (see `.env`) | `DEST_SSH_USER` (see `.env`) |
| PEM file | `SOURCE_SSH_PKEY_FILE` (see `.env`) | `DEST_SSH_PKEY_FILE` (see `.env`) |
| RDS Host | `SOURCE_RDS_HOST` (see `.env`) | `DEST_RDS_HOST` (see `.env`) |
| DB Name | `quest_rearch_production` | `quest_ple_analytics` |
| DB User | `SOURCE_DB_USER` (see `.env`) | `DEST_DB_USER` (see `.env`) |

All connection details are stored in `.env` (gitignored). Copy `.env.example` and fill in the values.  
PEM files must be placed in `ael_v2_pipeline/DB_Config/` (gitignored).

### Running for the First Time
```bash
cd "AEL V2/ael_v2_pipeline"
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
# fill in SOURCE_DB_PASSWORD and DEST_DB_PASSWORD in .env
# copy .pem files into DB_Config/
python main.py --user-id <test-user-uuid> --dry-run
```

---

## Output Schema (Columns in Order)

### Lesson-level (`main_learning_activity_myquest_ael_lesson`)
```
user_id, user_name, user_type, centre_id, project_id,
batch_id, trade_id, career_path_id, career_path_name,
subject_id, subject_name, subject_is_ple, ple_career_path_id,
year_to_map, trade_duration, subject_order,
lesson_id, lesson_name, lesson_order, lesson_type,
is_assessment, toolkit_type, allocation_path,
score, rating, duration, data_from, completed,
total_allocated, total_lessons_allocated, total_assessments_allocated,
total_completed, total_lessons_completed, total_assessments_completed,
completion_pct,
subj_total_allocated, subj_lessons_allocated, subj_assessments_allocated,
subj_total_completed, subj_lessons_completed, subj_assessments_completed
```

### Subject-level (`main_learning_activity_myquest_ael`)
```
user_id, user_name, user_type, centre_id, project_id,
batch_id, trade_id, career_path_id, career_path_name,
subject_id, subject_name, subject_is_ple, year_to_map, allocation_basis,
total_allocated, total_lessons_allocated, total_assessments_allocated,
total_completed, total_lessons_completed, total_assessments_completed,
completion_pct,
subj_total_allocated, subj_lessons_allocated, subj_assessments_allocated,
subj_total_completed, subj_lessons_completed, subj_assessments_completed,
avg_score, avg_rating, avg_duration, total_duration
```

Key notes:
- `completed = 1` for normal rows; `completed = 0` for zero-completion stub rows (lesson fields NULL)
- `data_from` is always `NULL` (not in source tables)
- `trade_id` is NULL for PLE users; `career_path_id`/`career_path_name` are NULL for non-PLE users
- `total_allocated` counts all allocated lessons pre-filter (full picture); split into lessons + assessments
- `duration` is `SUM(duration)` per `(user_id, lesson_id)` from `learning_activities` (seconds)
- `completion_pct` = `(total_completed / total_allocated) × 100` rounded to 2dp

---

## Debug Outputs (Temporary — to be removed)

- **`allocation_debug_<tag>_<ts>.csv`** — saved automatically on every run alongside the main output. Contains the full allocation DataFrame *before* the completion filter so you can verify total allocated lessons, check `allocation_basis`, and confirm `centre_id`, `batch_id`, `trade_id`, `career_path_id` are correct.
- Columns: `user_id`, `user_name`, `user_type`, `centre_id`, `batch_id`, `trade_id`, `career_path_id`, `career_path_name`, `subject_id`, `subject_name`, `year_to_map`, `trade_duration`, `subject_order`, `lesson_id`, `lesson_name`, `lesson_order`, `lesson_type`, `is_assessment`, `toolkit_type`, `allocation_path`, `allocation_basis`
- `allocation_basis` values:
  - non-PLE: `"centre_subject → batch_subject → subject_trade"`
  - PLE: `"centre_subject → subject_ple_career_path → batch_subject"`
- Remove `_save_allocation_debug()` call from `main.py` once analysis is complete.

---

## Possible Next Steps (Not Started)

These were not discussed yet — bring them up in the next session if needed:

1. **Scheduling / automation** — run the pipeline on a cron schedule (daily/weekly) and push to DB automatically
2. **Full run validation** — run `python main.py --dry-run` for all users and verify total row counts against the old Talend output
3. **Additional user types** — currently only types 3 & 4. If facilitators (other types) need allocation tracking, `s2_allocation.py` would need a separate query (no `student_access=1` filter, different table routing)
4. ~~**Output table rename**~~ — Done. Tables are now `main_learning_activity_myquest_ael` (subject) and `main_learning_activity_myquest_ael_lesson`
5. **Error alerting** — email/Slack notification if the pipeline fails or returns zero rows
6. **Incremental runs** — currently full replace on every run. Could add a `--since` date flag to only process users with recent activity
7. **Logging to file** — currently logs to stdout only. Could add `logging.FileHandler` for persistent run logs

---

## Test User

A PLE learner (type 3) with 2 active career paths was used to validate the pipeline. Real UUIDs are stored locally and must not be committed.

```
user_id : <redacted — see .env or ask team>
name    : <redacted>
type    : 3 (learner)
is_ple  : 1 (PLE user)
centre  : <redacted>
batch   : <redacted>
career paths: 2 active paths
```

After deduplication: 342 unique allocated lessons  
Expected output rows (completed=1): varies by actual completions in `learning_activities`


1. DEV_NOTES.md — inside your project folder
This is the main file to read before starting a new session. It contains:

Current status of every file (what's done, what's not)
All business logic decisions with explanations
Every bug fixed with root cause + fix
File-by-file function reference
Infrastructure details (bastion IPs, DB names, pem file names)
Full output column list
Possible next steps (automation, prod rename, full run validation, etc.)
Test user details
2. Claude Code memory files — auto-loaded in future sessions
Saved at ~/.claude/projects/.../memory/. Whenever you open this project in Claude Code again, I'll automatically remember:

What this pipeline does and where the dev log is
The SSH tunnel approach (no sshtunnel, custom paramiko)
All the business rules (completed=1 filter, year_to_map logic, dedup rule, source table routing)
Your working style and preferences
Next time, just open this folder in VS Code, start Claude Code, and say "continue the AEL V2 pipeline" — I'll load the context automatically.
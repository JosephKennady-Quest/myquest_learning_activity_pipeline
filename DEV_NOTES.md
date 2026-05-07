# AEL V2 Pipeline — Development Notes

> Running log of all decisions, bug fixes, and business logic choices made during development. Read this at the start of every new session to pick up exactly where we left off.

---

## What This Pipeline Does (Quick Summary)

Replaces the original Java/Talend ETL. For every active user (types 1–4), it produces:

- One row per **completed lesson** showing what was allocated, whether completed, score, rating, and per-subject counts
- One **stub row per user** for users with zero completions (lesson fields NULL, `completed = 0`) — every allocated user appears in the output
- A **subject-level aggregation** (one row per user × subject) with avg score/rating and counts
- A second subject-level aggregation with **all lesson types** included (no pdf/mp4/pdf web filter)

Output: DB tables in `quest_ple_analytics` (default) and/or CSV files.

| DB Table | Contents | Lesson type filter |
|---|---|---|
| `main_learning_activity_myquest_ael_lesson` | Lesson-level detail | pdf / mp4 / pdf web excluded |
| `main_learning_activity_myquest_ael` | Subject-level aggregation | pdf / mp4 / pdf web excluded |
| `main_learning_activity_myquest_ael_all_lesson_type` | Subject-level aggregation | All lesson types included |

---

## Current Status — What Is Done

| File | Status | Notes |
|---|---|---|
| `config.py` | Done | Loads `.env`; `LEARNER_TYPES (3,4)`, `STAFF_TYPES (1,2)`, `ALL_TYPES (1,2,3,4)`, `ALLOC_CHUNK_SIZE=2000`, `STAFF_ALLOC_CHUNK_SIZE=200` |
| `db.py` | Done | Custom SSH tunnel (paramiko, no sshtunnel); fetch / write_table / delete_user_rows helpers |
| `steps/s0_changed_users.py` | Done | Incremental mode: finds user_ids with new completions since a timestamp |
| `steps/s1_users.py` | Done | Fetches all active users types 1–4; LEFT JOIN student_details; supports user_id, centre_id, batch_id, trade_id filters |
| `steps/s2_allocation.py` | Done | Three paths: non_ple, ple, staff; optional batch/trade intersection; `paths` param; `_concat()` helper |
| `steps/s3_completion.py` | Done | Routes to correct table by user_type; `WHERE completed = 1`; subject counts; zero-completion stubs; auto-refetch on empty completion |
| `main.py` | Done | All flags wired; learner/staff separate chunks; three DB tables; `--output db` default; CSV gated on `--output csv/both` |
| `README.md` | Done | Full technical documentation — updated this session |
| `DEV_NOTES.md` | This file | Development log |

**Pipeline runs successfully.** All three DB tables written on every run.

---

## Business Logic — Key Decisions

### User Types (expanded from original types 3 & 4 only)

All four user types are now fetched in Step 1 and allocated in Step 2:

| Type | Role | Allocation path | Lesson access |
|---|---|---|---|
| 1 | Admin | `staff` | All lessons in the centre |
| 2 | Facilitator / Master Trainer | `staff` | `facilitator_access=1` or `mastertrainer_access=1` |
| 3 | Learner | `non_ple` or `ple` | `student_access=1` |
| 4 | Alumni | `non_ple` or `ple` | `student_access=1` |

`users.is_master_trainer` distinguishes Facilitator (NULL or 0) from Master Trainer (1) within type 2.

### Three Allocation Paths

**Non-PLE** (`users.is_ple IS NULL or != 1`, types 3 & 4):
- Base: `centre_subject`
- Optionally intersects: `batch_subject` (if `batch_id` not NULL) and `subject_trade` (if `trade_id` not NULL)
- `s.is_ple IN (0, 2)` — QuestApp and shared subjects only

**PLE** (`users.is_ple = 1`, types 3 & 4):
- Base: `centre_subject`
- Optionally intersects: `subject_ple_career_path` (if career path exists) and `batch_subject` (if `batch_id` not NULL)
- Career path: latest active `ple_career_path_user` row (ROW_NUMBER by updated_at DESC)
- `s.is_ple IN (1, 2)` — MyQuest and shared subjects only

**Staff** (types 1 & 2):
- Only: `centre_subject` — no batch, trade, or career path
- `s.is_ple IN (0, 1, 2)` — all platforms
- Access filter in WHERE: Admin gets all lessons; Facilitator needs `facilitator_access=1`; Master Trainer needs `mastertrainer_access=1`
- Completion stored in `facilitator_learning_activities`

### Optional Batch / Trade Intersection

Originally non-PLE required INNER JOINs on both `batch_subject` and `subject_trade`, which excluded users with NULL `batch_id` or `trade_id` entirely.

Changed to LEFT JOIN with NULL guards on the ON clause + conditional WHERE:

```sql
LEFT JOIN batch_subject bs
    ON  sd.batch_id IS NOT NULL      -- MySQL skips scan when key is NULL
    AND bs.batch_id   = sd.batch_id
    AND bs.subject_id = cs.subject_id

WHERE ...
  AND (sd.batch_id IS NULL OR bs.subject_id IS NOT NULL)
```

This implements "if the key exists, enforce the intersection; if it's NULL, let the subject through." The NULL guard on the ON clause prevents full-table scans when the key is absent, keeping performance equivalent to the original INNER JOIN for users who do have the key.

Same pattern applied to `trade_id` and to `pcp.id` (career path) in the PLE path.

### Separate Learner / Staff Chunking

Before this change, all four user types were mixed into learner-sized chunks. Admin users (type 1) return orders-of-magnitude more rows per user than learners, causing some chunks to OOM or run for hours.

Fix:
1. After Step 1, split `user_ids` into `learner_ids` (types 3,4) and `staff_ids` (types 1,2)
2. Learner chunks: `ALLOC_CHUNK_SIZE = 2000`; only run `non_ple` + `ple` paths (2 queries/chunk)
3. Staff chunks: `STAFF_ALLOC_CHUNK_SIZE = 200`; only run `staff` path (1 query/chunk)
4. `fetch_allocation()` accepts `paths=("non_ple", "ple", "staff")` to skip irrelevant queries

### `paths` Parameter on `fetch_allocation()`

Added to avoid running all three allocation SQL queries when only a subset is needed:
- Learner chunks pass `paths=("non_ple", "ple")`
- Staff chunks pass `paths=("staff",)`

This is purely a performance optimisation — the output is identical to running all three and discarding empty results.

### DB Tables — Three Tables Always Written

| Table | Lesson type filter | Write timing |
|---|---|---|
| `main_learning_activity_myquest_ael_lesson` | Excludes pdf/mp4/pdf web | Per chunk, during loop |
| `main_learning_activity_myquest_ael` | Excludes pdf/mp4/pdf web | Per chunk, during loop |
| `main_learning_activity_myquest_ael_all_lesson_type` | All types included | Once after loop |

The all-lesson-types table is built by accumulating unfiltered `alloc` frames across all chunks into `alloc_all_frames`, then running `merge_completion(alloc_combined, pd.DataFrame())` after the loop. The empty DataFrame triggers the auto-refetch path in `merge_completion()`.

### Output Mode Gating (`--output db` is default)

```
--output db    → writes 3 DB tables only; no files in output/
--output csv   → writes CSV files only; no DB writes
--output both  → writes DB tables + CSV files
```

Debug CSV (`debug_alloc_*.csv`) and no-allocation users CSV (`no_allocation_users_*.csv`) are both gated on `output in ("csv", "both")`. Previously they wrote unconditionally, causing unexpected files even on `--output db` runs.

### CSV Filename Conventions

| Prefix | Contents |
|---|---|
| `lessons_filtered_<tag>_<ts>.csv` | Lesson-level, pdf/mp4/pdf web excluded |
| `subjects_filtered_<tag>_<ts>.csv` | Subject-level, pdf/mp4/pdf web excluded |
| `lessons_all_types_<tag>_<ts>.csv` | Lesson-level, all types (with `--all-lesson-types`) |
| `subjects_all_types_<tag>_<ts>.csv` | Subject-level, all types (with `--all-lesson-types`) |
| `debug_alloc_<tag>_<ts>.csv` | Pre-completion allocation (small runs only) |
| `no_allocation_users_<ts>.csv` | Users with no allocation found |

Tag format (from `_make_tag()`):
- `all_users` — no filters active
- `ctr_<8chars>` — centre filter
- `batch_<8chars>` — batch filter
- `subj_<8chars>` — subject filter
- `trade_<8chars>` — trade filter
- Parts are joined with `_` when multiple filters are active

### Lesson Type Filter (default output)

`_EXCLUDED_LESSON_TYPES = {"pdf", "mp4", "pdf web"}` — applied in `main.py` via `_apply_lesson_type_filter()` before `merge_completion()`.

Applied to: `main_learning_activity_myquest_ael_lesson`, `main_learning_activity_myquest_ael`, and all filtered CSV files.

**NOT applied to:** `main_learning_activity_myquest_ael_all_lesson_type` and `*_all_types_*.csv` files.

The `--all-lesson-types` flag only controls whether extra CSV files are written — the all-lesson-types DB table is always written unconditionally.

### Year-to-Map Filter

Applies to learner/alumni paths only (staff have no trade):
- `subjects.year_to_map <= trades.duration` — include subject only if within trade's year range
- Pass-through: `year_to_map IS NULL`, `year_to_map = 0`, `trades.duration IS NULL`

### `is_assessment` — Name-Based Detection

`CASE WHEN l.is_assessment = 1 OR UPPER(l.name) LIKE '%%ASSESSMENT%%' THEN 1 ELSE 0 END`

The `%%` escaping is required by pymysql — a single `%` is interpreted as a Python format specifier and causes a `ValueError`.

### Completion Source Table Routing

- Types 3, 4 → `learning_activities`
- Types 1, 2 → `facilitator_learning_activities`
- `data_from` does NOT exist in source tables → always `NULL AS data_from`

### duration Column

- Fetched as `SUM(duration)` per `(user_id, lesson_id)` from the activity table
- Subject level: `avg_duration` (mean per lesson) and `total_duration` (sum) in `_build_subject_agg()`
- Must be included in `completion_cols` in `merge_completion()` — forgetting it causes a `KeyError` in `_build_subject_agg()`

### Zero-Completion Stub Rows

Every allocated user appears in the output. Users with no `completed = 1` records get one stub row per allocated subject (subject context filled in, all lesson-level columns NULL, counts are 0). Added at the end of `merge_completion()`.

### No-Allocation Stubs (main.py)

Users in a chunk with no allocation rows at all get a stub row added in `main.py` (not in `merge_completion()`). These have `total_allocated = 0` and all counts = 0. Accumulated across chunks → `no_allocation_users_*.csv` (only when CSV output is active).

### PLE Career Path — Latest Active Only

`ple_career_path_user` is wrapped in a derived table using `ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY updated_at DESC) = 1`. Only the most recently updated active career path is joined. Python dedup in `fetch_allocation()` is a safety net.

---

## Bugs Fixed (with Root Causes)

### 1. `sshtunnel` AttributeError — `paramiko.DSSKey`
- `sshtunnel` 0.4.0 uses `paramiko.DSSKey` removed in paramiko 3.x
- Fix: replaced with custom paramiko local port forwarder in `db.py`

### 2. `pymysql` `sock=` TypeError
- Tried passing paramiko channel as `sock=` — not supported
- Fix: same local port forwarder

### 3. Unknown column `data_from`
- `learning_activities` has no `data_from` column
- Fix: `NULL AS data_from` in SQL template

### 4. pandas SQLAlchemy warning
- `pd.read_sql()` called with raw pymysql connection
- Fix: `cursor.execute()` + `pd.DataFrame(rows, columns=column_names)`

### 5. Duplicate lesson rows (PLE multi-career-path)
- User enrolled in 2 career paths → one row per career path per shared lesson
- Fix: SQL `ROW_NUMBER()` to select only the latest active career path; Python `drop_duplicates` as safety net

### 6. `ValueError: unsupported format character 'A'` on LIKE clause
- `LIKE '%ASSESSMENT%'` — pymysql treats `%A` as a Python format specifier
- Fix: `%%` escaping in all SQL LIKE patterns

### 7. `OperationalError: Unknown column 'duration'`
- Table existed with old schema (no `duration`). `TRUNCATE` preserves schema.
- Fix: changed `write_table()` to `DROP TABLE IF EXISTS` + `CREATE TABLE` on full refresh — schema always rebuilt from the current DataFrame

### 8. `duration` column missing from merge result
- `completion_cols` in `merge_completion()` didn't include `"duration"` — column dropped before LEFT JOIN
- Fix: added `"duration"` to `completion_cols` list

### 9. `KeyError: 'user_id'` in `fetch_allocation()` sort_values
- When both non_ple and ple return 0 rows, `_concat([])` returns `pd.DataFrame()` (no columns)
- Fix: early return in `fetch_allocation()` after concat: `if combined.empty: return combined`

### 10. `KeyError: 'subject_id'` in `_build_subject_agg()`
- No-allocation stub rows added in `main.py` don't have `subject_id` column when allocation was completely empty
- Fix: early return guards at top of `_build_subject_agg()`: `if "subject_id" not in df.columns or df.empty: return pd.DataFrame()`

### 11. `FutureWarning` from pandas concat (all-NA dtype mismatch)
- All-NA columns (e.g. `career_path_id` in non_ple frames) have dtype float64; same column in ple frames is object — concat raises FutureWarning about future dtype behaviour
- Fix: `_concat()` helper in `s2_allocation.py` casts all-NA columns to `object` before concat

### 12. Pipeline slowdown (staff mixed into learner chunks)
- Admin users (type 1) return many more rows per user than learners. Mixing them into learner-sized chunks caused some chunks to run for hours. Also: 3 queries per chunk instead of 2 (staff path was running even for learner-only chunks).
- Fix: separate learner/staff split before the loop; separate chunk sizes; `paths` parameter to skip irrelevant queries per chunk type

### 13. `NameError: name 'all_user_ids' is not defined`
- Variable `all_user_ids` was removed when splitting into `learner_ids`/`staff_ids`, but `_print_summary_chunked` call still referenced it
- Fix: `len(learner_ids) + len(staff_ids)`

### 14. `--all-lesson-types` CSV files not appearing
- `if all_lesson_types and n_chunks == 1` guard was always False after the learner/staff split (minimum 2 chunks even for a small run)
- Fix: accumulate `alloc_all_frames` per chunk; write after loop using `merge_completion(alloc_combined, pd.DataFrame())`; remove `n_chunks == 1` guard

### 15. Debug CSV and no-alloc CSV written even with `--output db`
- Both were written unconditionally (`if not dry_run`) — producing unexpected files in `output/` on DB-only runs
- Fix: gate both on `output in ("csv", "both")`

---

## File-by-File Reference

### `config.py`
```python
SOURCE_DB             # quest_rearch_production connection dict
ANALYTICS_DB          # quest_ple_analytics connection dict
LEARNER_TYPES         = (3, 4)
LEARNER_TYPES_SQL     = "3,4"
STAFF_TYPES           = (1, 2)
STAFF_TYPES_SQL       = "1,2"
ALL_TYPES             = (1, 2, 3, 4)
ALL_TYPES_SQL         = "1,2,3,4"
CHUNK_SIZE            = 5000    # DB insert batch size
ALLOC_CHUNK_SIZE      = 2000    # learner users per allocation query
STAFF_ALLOC_CHUNK_SIZE= 200     # staff users per allocation query
OUTPUT_DIR            = os.getenv("OUTPUT_DIR", "output")
DB_CONFIG_DIR         = <pipeline_dir>/DB_Config
```

### `db.py`
- `fetch(cfg, sql, params)` → DataFrame
- `write_table(cfg, df, table, if_exists="replace")` → None
- `delete_user_rows(cfg, table, user_ids)` → None
- Full refresh: `DROP TABLE IF EXISTS` + `CREATE TABLE` + batch INSERT (schema always rebuilt)
- Incremental: `DELETE WHERE user_id IN (...)` + INSERT (append)
- Each call opens/closes its own SSH tunnel — no connection pool

### `steps/s0_changed_users.py`
- `fetch_changed_user_ids(since)` → List[str]
- Queries `learning_activities.completed_at > since` to find users with new completions
- Returns empty list if no users — main.py exits early

### `steps/s1_users.py`
- `fetch_users(user_id, centre_id, batch_id, trade_id)` → DataFrame
- Source: `users LEFT JOIN student_details`
- Filter: `type IN (1,2,3,4)`, `status=1`, `deleted_at IS NULL`
- Staff users (types 1,2) will have NULL for all `student_details` columns — expected

### `steps/s2_allocation.py`
- `fetch_non_ple_allocation(user_ids, centre_id, batch_id, subject_id, trade_id)` → DataFrame
- `fetch_ple_allocation(user_ids, centre_id, batch_id, subject_id, trade_id)` → DataFrame
- `fetch_staff_allocation(user_ids, centre_id, subject_id)` → DataFrame
- `fetch_allocation(user_ids, ..., paths=("non_ple","ple","staff"))` → DataFrame (combined, deduped)
- `_concat(frames)` → DataFrame — helper that casts all-NA columns to object before concat (fixes FutureWarning)
- Non-PLE/PLE: LEFT JOIN with NULL guards for optional batch/trade/career_path intersection
- Staff: centre_subject only; access filter in WHERE per role
- Adds `allocation_path` (`non_ple` / `ple` / `staff`) and `allocation_basis` columns

### `steps/s3_completion.py`
- `fetch_student_completion(user_ids)` — from `learning_activities WHERE completed=1`
- `fetch_facilitator_completion(user_ids)` — from `facilitator_learning_activities WHERE completed=1`
- `fetch_completion(user_ids, user_types)` — auto-routes; fetches from both tables if both type groups present
- `merge_completion(allocation, completion)` — LEFT JOIN; per-user and per-subject counts; zero-completion stubs
  - If `completion` is empty DataFrame, auto-refetches scoped to users in `allocation` (used by post-loop all-lesson-types write)
  - Must include `"duration"` in `completion_cols` list inside this function

### `main.py`
**Constants:**
```python
OUTPUT_TABLE_LESSON      = "main_learning_activity_myquest_ael_lesson"
OUTPUT_TABLE_SUBJECT     = "main_learning_activity_myquest_ael"
OUTPUT_TABLE_SUBJECT_ALL = "main_learning_activity_myquest_ael_all_lesson_type"
_EXCLUDED_LESSON_TYPES   = {"pdf", "mp4", "pdf web"}
```

**Key functions:**
- `_apply_lesson_type_filter(df)` — removes rows whose `lesson_type` is in `_EXCLUDED_LESSON_TYPES`
- `_build_subject_agg(df)` — collapses lesson-level result to one row per (user × subject); has early return guard for missing `subject_id`
- `_make_tag(...)` — builds descriptive filename suffix from active filter args
- `_save_csv(df, ..., prefix)` — saves to `OUTPUT_DIR/<prefix>_<tag>_<ts>.csv`
- `_save_allocation_debug(df, ..., prefix)` — saves pre-completion allocation as CSV

**Main loop flow:**
```
Step 1: fetch_users → learner_ids + staff_ids → learner_chunks + staff_chunks
Loop over all_chunks (learner first, then staff):
    fetch_allocation(paths=("non_ple","ple") for learner OR ("staff",) for staff)
    append to alloc_all_frames (always — for post-loop all-types table)
    apply lesson type filter → alloc_filtered
    fetch_completion for allocated users
    merge_completion(alloc_filtered, compl) → result
    add no-allocation stubs for users with no allocation
    write result + subject_agg to DB (filtered tables) and/or CSV
After loop:
    concat alloc_all_frames → merge_completion(alloc_combined, pd.DataFrame())
    write to OUTPUT_TABLE_SUBJECT_ALL (always) and all-types CSVs (if --all-lesson-types)
    write no_allocation_users CSV (if CSV output active)
    print final summary
```

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

### First-Run Setup

```bash
cd "AEL V2/ael_v2_pipeline"
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
# fill in SOURCE_DB_PASSWORD and DEST_DB_PASSWORD
# copy .pem files into DB_Config/
python main.py --user-id <test-user-uuid> --output csv --dry-run
```

---

## Output Schema (Column Order)

### Lesson-level (`main_learning_activity_myquest_ael_lesson`)
```
user_id, user_name, user_type, is_master_trainer,
centre_id, project_id, is_ple, batch_id, trade_id,
career_path_id, career_path_name,
subject_id, subject_name, subject_is_ple, ple_career_path_id,
year_to_map, trade_duration, subject_order,
lesson_id, lesson_name, lesson_order, lesson_type,
is_assessment, toolkit_type, allocation_path, allocation_basis,
score, rating, duration, data_from, completed,
total_allocated, total_lessons_allocated, total_assessments_allocated,
total_completed, total_lessons_completed, total_assessments_completed,
completion_pct,
subj_total_allocated, subj_lessons_allocated, subj_assessments_allocated,
subj_total_completed, subj_lessons_completed, subj_assessments_completed
```

### Subject-level (`main_learning_activity_myquest_ael` and `..._all_lesson_type`)
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
- `data_from` is always NULL (not in source tables)
- `trade_id` is NULL for PLE users and staff; `career_path_id/name` are NULL for non-PLE users and staff
- `duration` = `SUM(duration)` per `(user_id, lesson_id)` — total time spent across all completed attempts (seconds)
- `completion_pct` = `(total_completed / total_allocated) × 100` rounded to 2dp
- `trade_duration` is NULL for staff (no trade join)

---

## Debug Outputs

Available only for small runs (single learner chunk + single staff chunk) when `--output csv` or `--output both` is active:

- **`debug_alloc_<tag>_<ts>.csv`** — raw allocation DataFrame before completion merge. Contains `allocation_basis`, `lesson_type`, `is_assessment`, `toolkit_type`, etc. for verifying allocation logic.
- **`debug_alloc_all_types_<tag>_<ts>.csv`** — same but without the lesson type filter (with `--all-lesson-types`).

Both files are written only when `"debug"` is in `--outputs` (default).

---

## Possible Next Steps

1. **Scheduling / automation** — cron daily incremental + monthly full refresh
2. **Full run validation** — compare total row counts against old Talend output
3. **Error alerting** — email/Slack notification on pipeline failure or zero-row output
4. **Incremental for all-lesson-types table** — currently uses `replace` mode (full rebuild) even on incremental runs; could be improved to DELETE + INSERT per changed user

---

## Test Users

Real UUIDs stored locally — must not be committed.

- PLE learner (type 3), 2 active career paths → validated dedup logic
- Non-PLE learner (type 3), has batch + trade → validated non-PLE path
- Staff Admin (type 1) → validated staff allocation (all centre lessons)
- Staff Facilitator (type 2, is_master_trainer=0) → validated `facilitator_access=1` filter
- Staff Master Trainer (type 2, is_master_trainer=1) → validated `mastertrainer_access=1` filter

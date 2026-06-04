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

## Session Notes

---

### 2026-06-04 — Result Buffer + Deduplication + Multiple Bug Fixes

**What was built this session:**

#### 1. `ResultBuffer` class (`steps/s0_cache.py`)

Eliminates ~1,270 SSH write connections per run by buffering all chunk results in DuckDB and flushing to analytics DB once at the end.

- `append(key, df)` — accumulates chunk result in a DuckDB buffer table (`_rbuf_lesson`, `_rbuf_subject`, `_rbuf_subject_all`)
- `flush(key, analytics_cfg, analytics_table, if_exists, stream_chunk=100_000)` — streams from DuckDB → analytics DB via `fetchmany()` in 100k-row batches (memory-safe for 50M+ rows)
- `drop_all()` — cleanup on error

Active for: full unscoped runs where `start_chunk == 1` and no `--since`.
Inactive for: `--since` incremental, `--start-chunk` resume, scoped runs.

**SSH connections per run (updated):**
| Step | Before cache | After cache | After result buffer |
|---|---|---|---|
| s2 allocation | ~4,410 | 0 (DuckDB) | 0 (DuckDB) |
| s3 completion | ~2,205 | 1 upfront | 1 upfront |
| DB writes | ~1,270 | ~1,270 | 2 (bulk flush) |
| **Total** | **~7,886** | **~1,280** | **~11** |

#### 2. `learning_activities` deduplication

Multiple attempts by the same user on the same lesson create duplicate `(user_id, lesson_id)` rows. Fixed at two levels:

**Full refresh** — `_dedup_completion_table(table)` runs after batch load + lesson filter:
```sql
CREATE TABLE _dedup AS
SELECT * EXCLUDE (_rn) FROM (
    SELECT *, ROW_NUMBER() OVER (
        PARTITION BY user_id, lesson_id
        ORDER BY completed_at DESC NULLS LAST
    ) AS _rn FROM {table}
) WHERE _rn = 1
```
Keeps only the most recent attempt per `(user_id, lesson_id)`.

**Incremental** — changed from simple append to upsert:
1. Deduplicate the new batch itself (if same pair appears twice in the incremental window)
2. DELETE existing DuckDB rows for all affected `(user_id, lesson_id)` pairs
3. INSERT the fresh (latest) records
Ensures cache always has exactly one row per user × lesson.

#### 3. Lesson filter moved from MySQL to DuckDB

The `lesson_id IN (subquery)` was running against production on every batch (468 times), causing each batch to take 3+ minutes.

Fix: batch SQL simplified to `WHERE completed=1 AND user_id IN (...)`. After all batches load, `_apply_lesson_filter_in_duckdb(table)` runs one DELETE using already-cached `lessons`, `subjects`, `centre_subject` tables — zero SSH, runs in milliseconds.

#### 4. Retry logic for lost MySQL connections

`_fetch_with_retry(sql, params, max_retries=3, base_wait=5.0)` wraps `db.fetch()` with exponential backoff on connection errors:
- Attempt 1 fails → wait 5s → retry
- Attempt 2 fails → wait 10s → retry
- Attempt 3 fails → wait 20s → retry
- Non-connection errors re-raised immediately

#### 5. `_ensure_varchar_nulls(df)` — module-level helper

All-NULL object columns get inferred as INT32 by DuckDB (no data to infer string type from). Applied before every `CREATE TABLE` call (first chunk/buffer only):
```python
for col in df.columns:
    if df[col].dtype == object and df[col].isna().all():
        df[col] = pd.array([pd.NA] * len(df), dtype=pd.StringDtype())
```
Used in both `AllocationCache.append()` and `ResultBuffer.append()`.

#### 6. `TableCache._sanitize(df)` — expanded

Originally only replaced zero-date strings. Now handles three MySQL → Python conversion problems:
| MySQL type | pymysql returns | Problem | Fix |
|---|---|---|---|
| DATETIME zero date | `'0000-00-00 00:00:00'` string | DuckDB rejects timestamp | Replace with `None` |
| DATETIME column | `datetime.datetime` objects | Mixed with float NaN → `.max()` TypeError | Convert to `datetime64` |
| DECIMAL column | `decimal.Decimal` objects | DuckDB infers narrow `DECIMAL(p,s)` → overflow on later batches | Convert to `float64` |

---

**Bugs fixed this session:**

#### Bug 20 — `_cache_eligible` excluded `--force-refresh`, cache never built
- `_cache_eligible = not _scoped and not since and not force_refresh` — when `--force-refresh` passed, entire cache block skipped
- Fix: removed `force_refresh` from `_cache_eligible`; it now only controls whether to reuse existing data

#### Bug 21 — `is_fresh()` checked metadata only, not actual DuckDB tables
- Partial/interrupted run could save metadata then fail — next run saw metadata, thought tables existed, tried to query missing tables
- Fix: `is_fresh()` now also checks `information_schema.tables` for each source table

#### Bug 22 — DuckDB timestamp error on MySQL zero dates
- `0000-00-00 00:00:00` (valid in MySQL) rejected by DuckDB with "timestamp field value out of range"
- Fix: `_sanitize()` replaces zero-date strings with `None` before DuckDB registration

#### Bug 23 — `datetime.max()` TypeError after zero-date sanitization
- After replacing zero dates with `None`, column mixed `datetime.datetime` + `float NaN` → pandas `.max()` raised `'>=' not supported`
- Fix: `_sanitize()` detects datetime-object columns and converts to `datetime64` via `pd.to_datetime(errors='coerce')`

#### Bug 24 — DuckDB `DECIMAL` overflow across batches
- DuckDB inferred `DECIMAL(6,4)` from first batch (all values < 10). Later batch had `score=100.0` → overflow
- Fix: `_sanitize()` detects `decimal.Decimal` objects and converts to `float64` → DuckDB uses `DOUBLE`

#### Bug 25 — DuckDB `INT32` inference on all-NULL columns in allocation + result cache
- `project_id` was all-NULL across 468 learner chunks → DuckDB inferred INT32. Staff chunk 469 had real UUID strings → cast failed
- Fix: `_ensure_varchar_nulls(df)` forces all-NULL object columns to `pd.StringDtype()` before CREATE TABLE

#### Bug 26 — Lost MySQL connection during `learning_activities` batch fetch
- Large batch queries timed out mid-fetch due to MySQL `wait_timeout`
- Fix: `_fetch_with_retry()` with 3 retries and exponential backoff (5s, 10s, 20s)

#### Bug 27 — Slow batch fetch due to `lesson_id` subquery on every batch
- `lesson_id IN (SELECT DISTINCT ... JOIN JOIN)` ran against production 468 times → 3+ min per batch
- Fix: removed subquery from batch SQL; lesson filter applied once in DuckDB after all batches load

---

**How to run going forward:**

```bash
# Normal daily/weekly run (incremental, all from DuckDB)
python3 main.py

# Only needed when: allocation tables changed, new server, or cache corrupted
python3 main.py --force-refresh

# Apply dedup to existing cache without re-fetching (one-time fix)
python3 -c "
import duckdb; con = duckdb.connect('cache.duckdb')
for t in ['learning_activities', 'facilitator_learning_activities']:
    con.execute(f'CREATE TABLE _d AS SELECT * EXCLUDE (_rn) FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY user_id, lesson_id ORDER BY completed_at DESC NULLS LAST) AS _rn FROM {t}) WHERE _rn = 1')
    con.execute(f'DROP TABLE {t}'); con.execute(f'ALTER TABLE _d RENAME TO {t}')
    print(f'{t} deduped')
con.close()
"

# After main pipeline completes
python3 main_wcc_json_v2.py
```

---

### 2026-06-03 — DuckDB Cache Layer + Bug Fixes

**What was built this session:**

#### 1. Local DuckDB Cache (`steps/s0_cache.py`)

Two-class cache layer that eliminates ~6,600 SSH connections per run down to ~9.

**`TableCache`** — caches individual source tables from `quest_rearch_production`:
- Tables: `users`, `student_details`, `subjects`, `lessons`, `lesson_types`, `trades`, `centre_subject`, `batch_subject`, `subject_trade`, `ple_career_paths`, `subject_ple_career_path`, `ple_career_path_user`
- `make_fetch_fn()` returns a DuckDB-backed drop-in for `db.fetch()` — auto-adapts `%s→?` and backtick→double-quote
- `is_fresh()` checks both metadata AND actual table existence in DuckDB (see bug fix #17 below)
- `refresh_completion_tables(incremental=True, batch_size=5000)`:
  - **Full refresh**: fetches `learning_activities` + `facilitator_learning_activities` in batches of 5,000 user_ids; filters to active users × active lessons × `completed=1`; stores `MAX(completed_at)` in `cache_meta`
  - **Incremental**: fetches only `WHERE completed_at > last_cached_ts`, appends to existing table, updates max_ts; skips if no new records

**`AllocationCache`** — caches the full allocation result per chunk:
- `allocation_changed()`: runs COUNT(*) on 8 watch tables, compares against snapshot — detects any allocation table change
- On each chunk: `append(df)` saves allocation result to `allocation_cache` in DuckDB
- On unchanged runs: `load_chunk(user_ids)` loads directly from DuckDB — skips even local JOIN queries

**SSH connections per run (before → after):**
| Step | Before | After |
|---|---|---|
| s1 users | 1 | 0 (DuckDB) |
| s2 allocation | ~4,410 | 0 (DuckDB) |
| s3 completion | ~2,205 | 1 upfront only |
| Change detection | 0 | 8 COUNT(*) |
| **Total** | **~6,616** | **~9** |

#### 2. `s1_users.py`, `s2_allocation.py`, `s3_completion.py` — `fetch_fn=None` parameter

All three steps accept an optional `fetch_fn` parameter. When passed (from `TableCache.make_fetch_fn()`), they query DuckDB instead of MySQL. When not passed, behaviour is completely unchanged (falls back to `db.fetch`). No logic changes to any existing step.

#### 3. `config.py` — chunk sizes increased
- `ALLOC_CHUNK_SIZE`: 500 → 2000
- `STAFF_ALLOC_CHUNK_SIZE`: 100 → 200

#### 4. `main_wcc_json_v2.py` + `steps/s4_users_project_phase_json.py` — new Step 4

One-row-per-user JSON output table (`quest_analytics.main_wcc_json`):
- Source: `main_users` LEFT JOIN `main_centre_project` LEFT JOIN `main_phases`
- Two JSON columns: `project_phase_combos` (prog, project, phase per user) and `subject_combos` (subject completion stats per user)
- Separate CLI: `python3 main_wcc_json_v2.py`

---

**Bugs fixed this session:**

#### Bug 16 — `TypeError: Expected numeric dtype, got object` in `_build_subject_agg`
- `avg_score`/`avg_rating`/`avg_duration` columns arrive as `object` dtype when all values in a chunk are NULL
- Fix: `pd.to_numeric(..., errors="coerce")` before `.round(2)`

#### Bug 17 — `_cache_eligible` excluded `--force-refresh`, so cache was never built
- `_cache_eligible = not _scoped and not since and not force_refresh` — when `--force-refresh` was passed, the entire cache block was skipped, meaning `cache.duckdb` was never populated
- Fix: removed `force_refresh` from `_cache_eligible`; `force_refresh` now only controls whether to reuse existing data, not whether to enter the cache block

#### Bug 18 — `is_fresh()` checked metadata only, not actual DuckDB tables
- A partial/interrupted first run could save metadata then fail — next run saw metadata, returned `is_fresh()=True`, and tried to query tables that didn't exist
- Fix: `is_fresh()` now also checks `information_schema.tables` to verify every source table actually exists in DuckDB

#### Bug 19 — DuckDB `ConversionException: Could not convert string to INT32` on `allocation_cache`
- `init_table()` created the schema from `sample_df.head(0)` (zero rows) — DuckDB inferred UUID columns as INT32 since there was no data to infer string type from
- Fix: merged `init_table` into `append()`; table is now created from actual data on first call so types are inferred correctly

---

**How to run — cache lifecycle:**

```bash
# First time on a new server (or to fully rebuild cache):
python3 main.py --force-refresh

# Every run after (incremental learning_activities, DuckDB for everything else):
python3 main.py

# Cache file corrupted — delete and rebuild:
rm cache.duckdb
python3 main.py --force-refresh
```

---

## Current Status — What Is Done

| File | Status | Notes |
|---|---|---|
| `config.py` | Done | Loads `.env`; `ALLOC_CHUNK_SIZE=2000`, `STAFF_ALLOC_CHUNK_SIZE=200` |
| `db.py` | Done | Custom SSH tunnel (paramiko); fetch / write_table / delete_user_rows |
| `steps/s0_cache.py` | Done | `TableCache` + `AllocationCache` + `ResultBuffer`; incremental upsert for learning_activities; dedup by (user_id, lesson_id); retry logic; zero-date + DECIMAL + NULL-dtype sanitization |
| `steps/s0_changed_users.py` | Done | Incremental mode: finds user_ids with new completions since a timestamp |
| `steps/s1_users.py` | Done | Fetches all active users types 1–4; `fetch_fn=None` for DuckDB |
| `steps/s2_allocation.py` | Done | Three paths: non_ple, ple, staff; `fetch_fn=None` threaded through all functions |
| `steps/s3_completion.py` | Done | Routes by user_type; `fetch_fn=None` for DuckDB; zero-completion stubs |
| `steps/s4_users_project_phase_json.py` | Done | One-row-per-user JSON builder for `main_wcc_json` |
| `main.py` | Done | All flags including `--force-refresh`; cache layer wired; learner/staff separate chunks |
| `main_wcc_json_v2.py` | Done | CLI for Step 4 JSON output |
| `cache.duckdb` | Runtime | Auto-created on first run; gitignored; rebuild with `--force-refresh` |
| `README.md` | Done | Full technical documentation including DuckDB cache section |
| `DEV_NOTES.md` | This file | Development log |

**Pipeline runs successfully.** All three DB tables written on every run. DuckDB cache reduces SSH connections from ~6,616 to ~9 per run.

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

### First-Run Setup (Updated)

```bash
# 1. Pull code
git pull origin main

# 2. Install dependencies (includes duckdb)
pip install -r requirements.txt

# 3. Set up credentials
cp .env.example .env
nano .env   # fill in SOURCE_DB_PASSWORD and DEST_DB_PASSWORD

# 4. Copy .pem files into DB_Config/ (run on your Mac)
scp -i /path/to/server-key.pem DB_Config/*.pem joseph@<server-ip>:.../DB_Config/
chmod 400 DB_Config/*.pem

# 5. First run — builds full DuckDB cache + runs pipeline
python3 main.py --force-refresh

# 6. Every run after
python3 main.py
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

## Allocation Changes vs Incremental Mode — Known Limitation

### The problem

`--since` (incremental mode) only catches users with new **completions** (queries `learning_activities.completed_at`). It does NOT detect changes to allocation structure:

- New subject added to `centre_subject` → users' `total_allocated` stays stale
- Lesson removed from a subject → stale lesson rows remain in the DB
- Batch / trade / career path mapping changed → affected users not reprocessed

Users who haven't completed anything since the allocation change are silently skipped.

### Current recommended fix (no code change required)

Whenever allocation changes in the admin panel, run a scoped full refresh for the affected scope:

```bash
python main.py --centre-id <uuid> --output db    # allocation changed for a centre
python main.py --batch-id <uuid> --output db     # allocation changed for a batch
python main.py --trade-id <uuid> --output db     # allocation changed for a trade
```

This uses `db_write_mode = "replace"` on first chunk, correctly overwriting stale rows for all users in that scope.

### Future improvement (Solution 2)

If `centre_subject`, `batch_subject`, `subject_trade`, and `subject_ple_career_path` tables gain `updated_at`/`deleted_at` columns, a new `fetch_allocation_changed_user_ids(since)` function in `s0_changed_users.py` could detect affected users automatically and union them into `changed_ids`. This would make incremental mode fully self-correcting without any manual intervention.

---

## Possible Next Steps

1. **Scheduling / automation** — cron daily incremental + monthly full refresh (see README → Handling Allocation Changes)
2. **Full run validation** — compare total row counts against old Talend output
3. **Error alerting** — email/Slack notification on pipeline failure or zero-row output
4. **Incremental for all-lesson-types table** — currently uses `replace` mode (full rebuild) even on incremental runs; could be improved to DELETE + INSERT per changed user
5. **Allocation change detection (Solution 2)** — if allocation tables get `updated_at`, extend `s0_changed_users.py` to auto-detect affected users and include them in incremental runs

---

## Test Users

Real UUIDs stored locally — must not be committed.

- PLE learner (type 3), 2 active career paths → validated dedup logic
- Non-PLE learner (type 3), has batch + trade → validated non-PLE path
- Staff Admin (type 1) → validated staff allocation (all centre lessons)
- Staff Facilitator (type 2, is_master_trainer=0) → validated `facilitator_access=1` filter
- Staff Master Trainer (type 2, is_master_trainer=1) → validated `mastertrainer_access=1` filter

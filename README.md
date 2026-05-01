# AEL V2 Pipeline — User Learning Allocation & Completion

A Python data pipeline that tracks **user-level learning activity allocation and completion** for QuestAlliance's AEL (Accelerated Education and Learning) programme. This is the V2 rewrite of the original Talend/Java ETL pipeline.

---

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Configuration](#configuration)
- [Project Structure](#project-structure)
- [Pipeline Steps](#pipeline-steps)
  - [Step 1 — User Fetch](#step-1--user-fetch-s1_userspy)
  - [Step 2 — Toolkit Allocation](#step-2--toolkit-allocation-s2_allocationpy)
  - [Step 3 — Completion & Merge](#step-3--completion--merge-s3_completionpy)
- [Allocation Logic](#allocation-logic)
  - [Non-PLE Path](#non-ple-path)
  - [PLE Path](#ple-path)
  - [Year-to-Map Filtering](#year-to-map-filtering)
  - [Data Quality Rules](#data-quality-rules)
- [Completion Logic](#completion-logic)
  - [Source Table Routing](#source-table-routing)
  - [Completed = 1 Filter](#completed--1-filter)
- [Output Schema](#output-schema)
- [Running the Pipeline](#running-the-pipeline)
  - [All Users — Full Refresh](#all-users--full-refresh)
  - [Single User](#single-user)
  - [Filter Options](#filter-options-1)
  - [Output Options](#output-options)
  - [Controlling Which Files Are Written](#controlling-which-files-are-written)
  - [Dry Run](#dry-run)
- [Incremental Runs](#incremental-runs)
  - [How It Works](#how-it-works)
  - [When to Use](#when-to-use-incremental-vs-full-refresh)
  - [Example Commands](#example-commands)
  - [Recommended Daily Schedule](#recommended-daily-schedule)
  - [First-Run Requirement](#first-run-requirement)
- [Large Runs and Chunked Processing](#large-runs-and-chunked-processing)
- [Database Reference](#database-reference)
- [Understanding toolkit_type](#understanding-toolkit_type)
- [SSH Tunnel Architecture](#ssh-tunnel-architecture)
- [Troubleshooting](#troubleshooting)

---

## Overview

This pipeline answers the question:

> **For every active learner/alumni — what toolkit content (subjects + lessons) are they allocated, and of that allocated content, how much have they completed?**

It handles two distinct user populations:

| Population | Filter | Allocation logic |
|---|---|---|
| **Non-PLE** | `users.is_ple` IS NULL or `!= 1` | Centre → Batch → Trade subject intersection |
| **PLE** | `users.is_ple = 1` | Centre → Career Path → Batch subject intersection |

Both populations are run independently and merged into a single output with an `allocation_path` column for traceability. The final output contains **only completed lessons** (`completed = 1`) — unstarted allocation rows are excluded, and any completions outside the allocated set are also excluded by the LEFT JOIN design.

---

## Architecture

```
quest_rearch_production (source DB — read only)
        │
        ├── users + student_details      → Step 1: user list
        │
        ├── centre_subject               ┐
        ├── batch_subject                │ Step 2: allocation
        ├── subject_trade                │ (INNER JOINs enforce
        ├── subject_ple_career_path      │  only common records)
        ├── ple_career_path_user         │
        ├── ple_career_paths             │
        ├── subjects + lessons           │
        ├── lesson_types                 │
        └── trades                       ┘  ← year_to_map duration filter
        │
        ├── learning_activities             ┐ Step 3: completion
        └── facilitator_learning_activities ┘ (routed by user_type)

quest_analytics (analytics DB — write only)
        ├── main_learning_activity_myquest_ael         ← subject-level output
        └── main_learning_activity_myquest_ael_lesson  ← lesson-level output

                        ↓
              pandas merge (LEFT JOIN allocation × completion)
                        ↓
              filter: completed = 1 only
                        ↓
              ┌─────────────────────┐
              │  Final Output       │
              │  (CSV or DB table)  │
              └─────────────────────┘
```

---

## Prerequisites

- Python 3.11+
- SSH access to both bastion hosts (`.pem` key files):
  - `<source-key>.pem` — source DB bastion (<bastion ip>)
  - `<dest-key>.pem` — analytics DB bastion (<bastion ip>)
- Both `.pem` files must be placed inside `DB_Config/` within the pipeline folder:
  ```
  ael_v2_pipeline/
  ├── DB_Config/
  │   ├── <source-key>.pem
  │   └── <dest-key>.pem
  └── ...
  ```
- Network access to both bastion IPs (VPN or office network as required)

---

## Installation

```bash
# 1. Navigate to the pipeline folder
cd "AEL V2/ael_v2_pipeline"

# 2. Create and activate a virtual environment
python3 -m venv venv
source venv/bin/activate          # macOS / Linux
# venv\Scripts\activate           # Windows

# 3. Install dependencies
pip install -r requirements.txt
```

**Dependencies:**

| Package | Purpose |
|---|---|
| `pandas >= 2.0` | DataFrame operations, merging, CSV export |
| `pymysql >= 1.1` | MySQL driver — cursor-based queries and batch inserts |
| `paramiko >= 3.0` | SSH client for tunnel implementation (replaces sshtunnel) |
| `python-dotenv >= 1.0` | Loads `.env` credentials into environment variables |
| `SQLAlchemy >= 2.0` | Retained for compatibility; not used for active queries |

> **Why no `sshtunnel`?** The `sshtunnel` library (v0.4.0) references `paramiko.DSSKey` which was removed in paramiko 3.x. The pipeline uses a custom local port forwarder built directly on `paramiko` — see [SSH Tunnel Architecture](#ssh-tunnel-architecture).

---

## Configuration

Credentials live in `.env` — never commit this file.

```bash
cp .env.example .env
# then fill in the two passwords
```

Full `.env` reference:

```ini
# ── Source DB (quest_rearch_production) ──────────────────────
SOURCE_SSH_HOST=<bastion-ip>
SOURCE_SSH_PORT=22
SOURCE_SSH_USER=<ssh-username>
SOURCE_SSH_PKEY_FILE=<keyfile>.pem      # filename only — must be in DB_Config/

SOURCE_RDS_HOST=<rds-endpoint>
SOURCE_RDS_PORT=3306

SOURCE_DB_USER=<db-username>
SOURCE_DB_PASSWORD=                     # ← fill this in
SOURCE_DB_NAME=quest_rearch_production

# ── Destination DB (quest_ple_analytics) ──────────────────────
DEST_SSH_HOST=<bastion-ip>
DEST_SSH_PORT=22
DEST_SSH_USER=<ssh-username>
DEST_SSH_PKEY_FILE=<keyfile>.pem        # filename only — must be in DB_Config/

DEST_RDS_HOST=<rds-endpoint>
DEST_RDS_PORT=3306

DEST_DB_USER=<db-username>
DEST_DB_PASSWORD=                       # ← fill this in
DEST_DB_NAME=quest_analytics

# ── Output ────────────────────────────────────────────────────
OUTPUT_DIR=output
```

Copy `.env.example` to `.env` and fill in the real values. The `.pem` key filenames are resolved against `DB_Config/` inside the pipeline folder:

```
ael_v2_pipeline/
├── DB_Config/
│   ├── <source-key>.pem              ← paste your .pem files here (gitignored)
│   └── <dest-key>.pem
├── .env                              ← credentials (gitignored — never commit)
├── .env.example                      ← template (committed, no real values)
└── config.py                         ← reads .env, builds connection dicts
```

> **Never commit `.env`** — only `.env.example` (with blank passwords) belongs in version control.

---

## Project Structure

```
ael_v2_pipeline/
│
├── .env.example          # Template — copy to .env and fill passwords
├── requirements.txt      # Python dependencies
│
├── config.py             # Loads .env → builds SOURCE_DB, ANALYTICS_DB dicts
│                         # Also exposes: LEARNER_TYPES, LEARNER_TYPES_SQL,
│                         #               CHUNK_SIZE, OUTPUT_DIR, DB_CONFIG_DIR
│
├── db.py                 # Low-level DB helpers (SSH tunnel + MySQL):
│                         #   fetch()       — SELECT query → DataFrame
│                         #   write_table() — DataFrame → MySQL table (chunked)
│                         # Tunnel implemented as custom local port forwarder
│                         # (paramiko direct-tcpip, no sshtunnel library)
│
├── steps/
│   ├── __init__.py
│   │
│   ├── s1_users.py       # Fetch active learners + alumni (type IN 3,4)
│   │                     # with their student_details profile
│   │
│   ├── s2_allocation.py  # Core allocation logic:
│   │                     #   fetch_non_ple_allocation() — non-PLE users
│   │                     #   fetch_ple_allocation()     — PLE users
│   │                     #   fetch_allocation()         — both combined
│   │                     # Applies year_to_map <= trade.duration filter
│   │
│   └── s3_completion.py  # Completion data from source DB:
│                         #   fetch_student_completion()     — learning_activities
│                         #   fetch_facilitator_completion() — facilitator_learning_activities
│                         #   fetch_completion()             — auto-routes by user_type
│                         #   merge_completion()             — LEFT JOIN + summary + filter
│
├── main.py               # CLI entry point / orchestrator
│                         # Args: --user-id, --output (csv/db/both), --dry-run
│
├── DB_Config/            # SSH private key files (.pem) — NOT committed
└── output/               # Default CSV output directory
```

---

## Pipeline Steps

### Step 1 — User Fetch (`s1_users.py`)

Fetches all active learners and alumni with their student profile.

**Source tables:** `users`, `student_details`

**Filters applied:**
- `users.type IN (3, 4)` — learners (3) and alumni (4) only
- `users.status = 1` — active accounts only
- `users.deleted_at IS NULL` — non-deleted only

**Key fields returned:**

| Field | Source | Description |
|---|---|---|
| `user_id` | `users.id` | Primary identifier (UUID) |
| `user_type` | `users.type` | 3 = learner, 4 = alumni |
| `centre_id` | `users.centre_id` | Drives subject allocation |
| `is_ple` | `users.is_ple` | Routes to PLE or non-PLE allocation path |
| `batch_id` | `student_details.batch_id` | Used in batch_subject join |
| `trade_id` | `student_details.trade_id` | Used in subject_trade join (non-PLE) |

> **Note:** `student_details` is LEFT JOINed here (informational fetch only). Users without a `student_details` row will produce zero allocation rows in Step 2 because Step 2 uses INNER JOINs.

---

### Step 2 — Toolkit Allocation (`s2_allocation.py`)

Builds the full list of subjects and lessons each user is allocated, using INNER JOINs to enforce that only subjects common across all relevant mapping tables are included.

Three public functions:

```python
fetch_non_ple_allocation(
    user_id=None, user_ids=None,          # single user or list of users
    centre_id=None, batch_id=None,
    subject_id=None, trade_id=None,
)  # non-PLE users only

fetch_ple_allocation(
    user_id=None, user_ids=None,
    centre_id=None, batch_id=None,
    subject_id=None, trade_id=None,
)  # PLE users only

fetch_allocation(
    user_id=None, user_ids=None,
    centre_id=None, batch_id=None,
    subject_id=None, trade_id=None,
)  # both combined — use this in main.py
```

`user_ids` accepts a `List[str]` and is used by the chunked processing loop (see [Large Runs and Chunked Processing](#large-runs-and-chunked-processing)). It is mutually exclusive with `user_id`.

See [Allocation Logic](#allocation-logic) for full join chains and the year_to_map filter.

---

### Step 3 — Completion & Merge (`s3_completion.py`)

Fetches raw lesson activity data from the **direct source DB** (`quest_rearch_production`) and LEFT JOINs it onto the allocation result.

```python
fetch_student_completion(user_ids)                        # types 3, 4 → learning_activities (WHERE completed=1)
fetch_facilitator_completion(user_ids)                    # others → facilitator_learning_activities (WHERE completed=1)
fetch_completion(user_ids, user_types={3,4})              # auto-routes by type set; user_ids required
merge_completion(allocation_df, completion_df)            # LEFT JOIN + summary stats + zero-completion stubs
```

`main.py` automatically extracts the unique `user_type` values present in the allocation DataFrame and passes them to `fetch_completion()`, so the routing is transparent.

**What counts as completed:** A lesson is marked completed only when a record with `completed = 1` exists in the activity table for that `(user_id, lesson_id)` pair. Records that exist but have `completed != 1` (e.g. viewed-only or in-progress) are excluded at the SQL level before any merging.

**Final output:** Only `completed = 1` rows are returned. See [Completed = 1 Filter](#completed--1-filter).

---

## Allocation Logic

### Non-PLE Path

A subject is allocated to a non-PLE user only if it appears in **all three** of the following mappings for that user's specific centre, batch, and trade:

```
centre_subject   (centre_id  = user.centre_id)
    ∩
batch_subject    (batch_id   = student_details.batch_id)
    ∩
subject_trade    (trade_id   = student_details.trade_id)
```

**Full join chain:**

```sql
users
  JOIN student_details        ON user_id = users.id
                                 AND batch_id IS NOT NULL
                                 AND trade_id IS NOT NULL
  JOIN centre_subject         ON centre_id = users.centre_id
  JOIN batch_subject          ON batch_id  = student_details.batch_id
                                 AND subject_id = centre_subject.subject_id
  JOIN subject_trade          ON trade_id  = student_details.trade_id
                                 AND subject_id = centre_subject.subject_id
  LEFT JOIN trades t_trade    ON t_trade.id = student_details.trade_id
  JOIN subjects               ON id = centre_subject.subject_id
                                 AND status = 1 AND deleted_at IS NULL
  JOIN lessons                ON subject_id = subjects.id
                                 AND status = 1 AND deleted_at IS NULL
                                 AND student_access = 1
  LEFT JOIN lesson_types      ON id = lessons.lesson_type_id

WHERE users.type IN (3, 4)
  AND users.status = 1
  AND users.deleted_at IS NULL
  AND (users.is_ple IS NULL OR users.is_ple != 1)
  AND (subjects.year_to_map IS NULL
       OR subjects.year_to_map = 0
       OR t_trade.duration IS NULL
       OR subjects.year_to_map <= t_trade.duration)
```

---

### PLE Path

A subject is allocated to a PLE user only if it appears in **all three** of the following mappings for that user's specific centre, career path, and batch:

```
centre_subject            (centre_id       = user.centre_id)
    ∩
subject_ple_career_path   (ple_career_path_id = user's active career path)
    ∩
batch_subject             (batch_id        = student_details.batch_id)
```

The career path is resolved via:
```
ple_career_path_user  (user_id = users.id, status = 1, deleted_at IS NULL)
    → ple_career_paths (id = job_type_id, deleted_at IS NULL)
```

**Full join chain:**

```sql
users
  JOIN student_details          ON user_id = users.id
                                   AND batch_id IS NOT NULL
  JOIN ple_career_path_user     ON user_id = users.id
                                   AND status = 1 AND deleted_at IS NULL
  JOIN ple_career_paths         ON id = job_type_id
                                   AND deleted_at IS NULL
  JOIN centre_subject           ON centre_id = users.centre_id
  JOIN subject_ple_career_path  ON ple_career_path_id = ple_career_paths.id
                                   AND subject_id = centre_subject.subject_id
  JOIN batch_subject            ON batch_id  = student_details.batch_id
                                   AND subject_id = centre_subject.subject_id
  LEFT JOIN trades t_trade      ON t_trade.id = student_details.trade_id
  JOIN subjects                 ON id = centre_subject.subject_id
                                   AND status = 1 AND deleted_at IS NULL
  JOIN lessons                  ON subject_id = subjects.id
                                   AND status = 1 AND deleted_at IS NULL
                                   AND student_access = 1
  LEFT JOIN lesson_types        ON id = lessons.lesson_type_id

WHERE users.type IN (3, 4)
  AND users.status = 1
  AND users.deleted_at IS NULL
  AND users.is_ple = 1
  AND (subjects.year_to_map IS NULL
       OR subjects.year_to_map = 0
       OR t_trade.duration IS NULL
       OR subjects.year_to_map <= t_trade.duration)
```

---

### Year-to-Map Filtering

`subjects.year_to_map` controls which year of a multi-year trade programme a subject belongs to. The filter ensures a user only receives subjects appropriate for their trade's duration.

**Rule:** Include the subject only if `subjects.year_to_map <= trades.duration` for the user's trade.

| Trade duration | Subjects included |
|---|---|
| 1 year | Only subjects with `year_to_map = 1` (or NULL / 0 = always include) |
| 2 years | Subjects with `year_to_map = 1` **and** `year_to_map = 2` |

**Pass-through conditions** (subject is always included regardless of year_to_map):
- `subjects.year_to_map IS NULL` — subject has no year restriction
- `subjects.year_to_map = 0` — explicit "no restriction" flag
- `t_trade.duration IS NULL` — user's trade has no duration set (LEFT JOIN result is NULL)

For PLE users, if `student_details.trade_id` is NULL the trades join yields NULL and all subjects pass through.

---

### Subject Platform Filter (`subjects.is_ple`)

`subjects.is_ple` controls which platform and user type each subject is designed for:

| `subjects.is_ple` | Platform | Allocated to |
|---|---|---|
| `0` | QuestApp | Non-PLE users only |
| `1` | MyQuest | PLE users only |
| `2` | Both | All users |

This filter is applied per allocation path:
- **Non-PLE query**: `AND s.is_ple IN (0, 2)` — only QuestApp and shared subjects
- **PLE query**: `AND s.is_ple IN (1, 2)` — only MyQuest and shared subjects

---

### Data Quality Rules

Applied uniformly to both allocation paths:

| Entity | Rule |
|---|---|
| `users` | `status = 1` AND `deleted_at IS NULL` AND `type IN (3, 4)` |
| `student_details` | `batch_id IS NOT NULL` (non-PLE also requires `trade_id IS NOT NULL`) |
| `subjects` | `status = 1` AND `deleted_at IS NULL` AND `is_ple IN (0,2)` for non-PLE / `IN (1,2)` for PLE |
| `lessons` | `status = 1` AND `deleted_at IS NULL` AND `student_access = 1` AND `lesson_category_id = 'd78bc322-568f-4110-8e24-02ea444d48b7'` |
| `ple_career_paths` | `deleted_at IS NULL` |
| `ple_career_path_user` | `status = 1` AND `deleted_at IS NULL` |

**Orphan / partial record handling:** Because every allocation join is an `INNER JOIN`, any user missing a batch, trade, or career path mapping produces zero allocation rows — they are not included with partial data.

**Multi-career-path deduplication:** A PLE user enrolled in more than one active career path (multiple rows in `ple_career_path_user`) will produce one row per career path for every shared lesson. After combining PLE and non-PLE results, `fetch_allocation()` deduplicates on `(user_id, lesson_id)`, keeping the row from the **most recently updated** career path (`pcpu.updated_at DESC`). This ensures that when a user changes their career path, the latest one is always used. The `career_path_updated_at` helper column is dropped from the final output after dedup. The count of dropped duplicate rows is logged at INFO level.

---

## Completion Logic

### Source Table Routing

Completion data is fetched from `quest_rearch_production` directly (not from the analytics DB). The source table depends on the user type:

| User type | Source table | Notes |
|---|---|---|
| `3` (learner), `4` (alumni) | `learning_activities` | Primary learner activity table |
| All other types | `facilitator_learning_activities` | For facilitators, master trainers, etc. |

The query filters `WHERE completed = 1` at the SQL level, then aggregates per `(user_id, lesson_id)` pair:
- `MAX(score)` — best score across all completed attempts
- `MAX(rating)` — best rating across all completed attempts
- `NULL AS data_from` — this column does not exist in the source tables

`main.py` derives the exact `user_types` present in the allocation result and passes them to `fetch_completion()`, so the correct tables are always queried.

### Completed = 1 Filter

Completion is enforced at **two layers**:

**Layer 1 — SQL (source table filter):**  
The `_SQL` query in `s3_completion.py` includes `WHERE completed = 1`. Only rows where the activity table column `completed = 1` are fetched. Records that exist but have `completed = 0` or NULL (viewed-only, in-progress) are excluded before any data leaves the database.

**Layer 2 — Python (allocation × completion merge):**  
`merge_completion()` LEFT JOINs the completion data onto the allocation DataFrame. This means:

- A lesson **allocated** but **no `completed = 1` record** → `completed = 0` in the output (not started or only partially viewed)
- A lesson **allocated** and **has a `completed = 1` record** → `completed = 1` in the output
- A lesson completed by the user but **not in their allocation** → excluded entirely (LEFT JOIN from allocation side)

**Summary stats are computed on the full merged dataset** (before the Python-level filter) so that `total_allocated_lessons`, `total_completed_lessons`, and `completion_pct` accurately reflect the user's full picture. The final returned DataFrame then contains only `completed = 1` rows.

This two-layer approach ensures both data quality (no partial views inflating counts) and accurate allocation coverage tracking.

---

## Output Schema

### Lesson-Level Output (`main_learning_activity_myquest_ael_lesson`)

One row per **user × completed lesson**. Users with zero completions get a single stub row with lesson/subject fields NULL and `completed = 0`.

| Column | Type | Description |
|---|---|---|
| `user_id` | string | User UUID |
| `user_name` | string | Full name |
| `user_type` | int | 3 = learner, 4 = alumni |
| `centre_id` | string | Centre UUID |
| `project_id` | string | Project UUID |
| `batch_id` | string | Batch UUID |
| `trade_id` | string | Trade UUID (non-PLE only; NULL for PLE) |
| `career_path_id` | string | PLE career path UUID (PLE only; NULL for non-PLE) |
| `career_path_name` | string | PLE career path name (PLE only) |
| `subject_id` | string | Subject UUID (NULL for zero-completion stub rows) |
| `subject_name` | string | Subject name (NULL for zero-completion stub rows) |
| `subject_is_ple` | int | PLE flag on the subject (0 / 1 / 2) |
| `ple_career_path_id` | string | Career path linked directly on the subject |
| `year_to_map` | int | Year restriction on the subject (NULL = unrestricted) |
| `trade_duration` | int | Duration of the user's trade in years (NULL if not set) |
| `subject_order` | int | Display order from `centre_subject` |
| `lesson_id` | string | Lesson UUID (NULL for zero-completion stub rows) |
| `lesson_name` | string | Lesson name (NULL for zero-completion stub rows) |
| `lesson_order` | int | Display order within the subject |
| `lesson_type` | string | e.g. Video, PDF, Assessment |
| `is_assessment` | int | 1 = assessment lesson, 0 = learning content |
| `toolkit_type` | string | `student` / `facilitator` / `master` |
| `allocation_path` | string | `ple` or `non_ple` — which path allocated this row |
| `score` | float | Best score from `learning_activities` (NULL if not available) |
| `rating` | float | Best rating (NULL if not rated) |
| `data_from` | string | Always NULL — column not present in source activity tables |
| `completed` | int | `1` = lesson completed; `0` = zero-completion stub row |
| `total_allocated_lessons` | int | Total lessons allocated to this user (pre-filter) |
| `total_completed_lessons` | int | Lessons completed out of their allocation |
| `completion_pct` | float | `(completed / allocated) × 100`, rounded to 2 dp |
| `subj_total_allocated` | int | Total lessons allocated in this subject for this user |
| `subj_lessons_allocated` | int | Non-assessment lessons allocated in this subject |
| `subj_assessments_allocated` | int | Assessment lessons allocated in this subject |
| `subj_total_completed` | int | Lessons completed in this subject |
| `subj_lessons_completed` | int | Non-assessment lessons completed in this subject |
| `subj_assessments_completed` | int | Assessment lessons completed in this subject |

**Zero-completion stub rows:** Users allocated lessons but with no `completed = 1` records appear as a single row with `completed = 0`. All lesson and subject-specific columns (`subject_id`, `lesson_id`, `lesson_name`, `score`, etc.) are NULL. The user-level stats (`total_allocated_lessons`, `total_completed_lessons = 0`, `completion_pct = 0.0`) are filled in. This ensures every allocated user appears in the output even if they haven't started any lessons.

---

### Subject-Level Aggregation (`main_learning_activity_myquest_ael`)

One row per **user × subject**. This is the primary analytics table. Zero-completion users are excluded (no subject to aggregate on).

| Column | Type | Description |
|---|---|---|
| `user_id` | string | User UUID |
| `user_name` | string | Full name |
| `user_type` | int | 3 = learner, 4 = alumni |
| `centre_id` | string | Centre UUID |
| `project_id` | string | Project UUID |
| `batch_id` | string | Batch UUID |
| `trade_id` | string | Trade UUID (non-PLE only) |
| `career_path_id` | string | PLE career path UUID |
| `career_path_name` | string | PLE career path name |
| `subject_id` | string | Subject UUID |
| `subject_name` | string | Subject name |
| `subject_is_ple` | int | PLE flag (0 / 1 / 2) |
| `year_to_map` | int | Year restriction on the subject |
| `allocation_basis` | string | Which mapping tables allocated this subject |
| `total_allocated_lessons` | int | User's total allocated lessons across all subjects |
| `total_completed_lessons` | int | User's total completed lessons across all subjects |
| `completion_pct` | float | User-level completion percentage |
| `subj_total_allocated` | int | Total lessons allocated in this subject |
| `subj_lessons_allocated` | int | Non-assessment lessons allocated |
| `subj_assessments_allocated` | int | Assessment lessons allocated |
| `subj_total_completed` | int | Lessons completed in this subject |
| `subj_lessons_completed` | int | Non-assessment lessons completed |
| `subj_assessments_completed` | int | Assessment lessons completed |
| `avg_score` | float | Average score across completed assessments in this subject |
| `avg_rating` | float | Average rating across completed lessons in this subject |

---

## Running the Pipeline

### All Users — Full Refresh

Runs for every active learner and alumni. Writes results to the analytics DB (default).  
On first run, or when you want a complete rebuild, use the full refresh (no `--since`).

```bash
cd ael_v2_pipeline
source venv/bin/activate
python main.py
```

> **Full refresh** TRUNCATEs both output tables and rewrites every row. On a large deployment (900K+ users) this takes ~5 hours.

### Single User

Returns the complete allocation and completion dataset for one user. Useful for testing and debugging without a full run.

```bash
python main.py --user-id 04d9c06e-1a37-428e-9b38-c0243b86544d --output csv
```

### Filter Options

All filters are optional and can be freely combined:

| Flag | Filters on | Example |
|---|---|---|
| `--user-id <uuid>` | Single user | `--user-id 04d9c06e-...` |
| `--centre-id <uuid>` | All users in a centre | `--centre-id 0dd48495-...` |
| `--batch-id <uuid>` | All users in a batch | `--batch-id 096b21a6-...` |
| `--subject-id <uuid>` | Only one specific subject | `--subject-id abc123-...` |
| `--trade-id <uuid>` | Only users in a specific trade | `--trade-id def456-...` |

`--subject-id` is applied at the SQL level in both allocation queries (`AND s.id = %s`).  
`--trade-id` is applied at the SQL level as `AND sd.trade_id = %s` — note that PLE users without a trade will return zero rows when this filter is active.

The startup log line shows all active filters:
```
[user_id=ALL | centre_id=0dd48495-... | batch_id=ALL | subject_id=ALL | trade_id=ALL | output=db | outputs=lesson,subject,debug | dry_run=False]
```

### Output Options

| Flag | Behaviour |
|---|---|
| `--output db` | Write to analytics DB tables — **default** |
| `--output csv` | Write CSV files to `OUTPUT_DIR/` |
| `--output both` | Write both DB tables **and** CSV files |

**DB tables written (in `quest_ple_analytics`):**

| Table | Contents |
|---|---|
| `main_learning_activity_myquest_ael` | Subject-level aggregation (one row per user × subject) |
| `main_learning_activity_myquest_ael_lesson` | Lesson-level detail (one row per user × completed lesson) |

Both tables are written when `--output db` or `--output both` is used (controlled by `--outputs`).

```bash
python main.py                                               # all users, DB output (full refresh)
python main.py --output csv                                  # all users, CSV only
python main.py --output both                                 # CSV + DB
python main.py --user-id <uuid> --output csv                 # single user to CSV
python main.py --centre-id <uuid> --output db                # centre to DB
python main.py --centre-id <uuid> --batch-id <uuid>          # combined filter
python main.py --trade-id <uuid>                             # all users in a trade
python main.py --subject-id <uuid>                           # all users, single subject only
python main.py --since "2026-04-30 00:00:00"                 # incremental: only changed users
```

### Controlling Which Files Are Written

```bash
# Default: write lesson detail, subject aggregation, and debug CSV
python main.py --outputs lesson,subject,debug

# Skip debug output (pre-completion allocation CSV)
python main.py --outputs lesson,subject

# Only write the subject-level aggregation
python main.py --outputs subject

# Include unfiltered variants (with pdf/mp4/pdf web lesson types)
python main.py --all-lesson-types
```

### Dry Run

Prints row counts and a summary without writing any output. Use this to validate DB connectivity and allocation logic before a full run.

```bash
python main.py --dry-run
python main.py --user-id <uuid> --dry-run
python main.py --centre-id <uuid> --dry-run
```

**Sample console output (single-chunk / scoped run):**

```
────────────────────────────────────────────────────────────
  Users processed       : 4,821  (PLE: 312 | non-PLE: 4,509)
  Unique subjects       : 148
  Unique lessons        : 2,034
  Total rows            : 97,210
  Avg completion        : 43.7%
────────────────────────────────────────────────────────────
```

**Sample console output (full run — 900K+ users, multi-chunk):**

```
────────────────────────────────────────────────────────────
  Users fetched         : 931,035
  Users in output       : 931,035  (PLE: 12,480 | non-PLE: 903,291 | no-alloc: 15,264)
  Avg completion        : 31.4%
────────────────────────────────────────────────────────────
```

> In the multi-chunk summary, `no-alloc` counts users who appear in the `users` table but have no allocation (missing batch, trade, or career path mapping). They appear as single stub rows in the output.

---

## Incremental Runs

After a full refresh has been run at least once, you can use **incremental mode** to update only the users who have new completions since a given timestamp. Incremental runs typically finish in minutes rather than hours.

### How It Works

Incremental mode is activated by the `--since` flag:

```bash
python main.py --since "YYYY-MM-DD HH:MM:SS"
```

The pipeline executes a pre-step (Step 0) that queries `learning_activities.completed_at` in the source database to find every `user_id` where at least one row satisfies:

```sql
SELECT DISTINCT user_id
FROM   learning_activities
WHERE  completed    = 1
  AND  completed_at > '<since timestamp>'
```

Only those users are then fetched, allocated, and re-processed. All other users already have correct rows in the analytics DB from the last run — their rows are never touched.

**Write strategy for incremental mode:**

For each chunk of changed users:
1. `DELETE FROM <table> WHERE user_id IN (...)` — removes the user's existing rows
2. `INSERT` the freshly-computed rows for those users

This means:
- Users with new completions get their rows fully replaced with up-to-date data
- Users with no new completions keep their existing rows unchanged
- No TRUNCATE is performed — the tables grow/update rather than being rebuilt

**What gets updated:**
- Both `main_learning_activity_myquest_ael_lesson` (lesson-level) and `main_learning_activity_myquest_ael` (subject-level) are updated together

**If a changed user's allocation also changed** (e.g. they moved batch or centre): their existing rows are deleted and all new rows (including the new allocation) are inserted.

---

### When to Use: Incremental vs Full Refresh

| Scenario | Recommended mode |
|---|---|
| First time running the pipeline | Full refresh (`python main.py`) |
| Daily scheduled run | Incremental (`--since "yesterday 00:00:00"`) |
| A batch or subject mapping was changed | Full refresh — allocation data changed |
| A large number of users had completions | Either — incremental is still faster |
| Output tables were accidentally dropped | Full refresh — tables need to be rebuilt |
| Testing one user | Single user: `--user-id <uuid>` (no `--since` needed) |

> **Tip:** If you are unsure whether just completion data or also allocation data changed, run a full refresh. Full refresh is always safe (it rebuilds everything from scratch). Incremental is just faster when only completions change.

---

### Example Commands

```bash
# Incremental: process users with completions since midnight today
python main.py --since "2026-04-30 00:00:00"

# Incremental: last 7 days (catch-up after a missed run)
python main.py --since "2026-04-23 00:00:00"

# Incremental: scoped to a single centre (only changed users in that centre)
python main.py --since "2026-04-30 00:00:00" --centre-id <uuid>

# Incremental dry run — see how many users would be updated without writing
python main.py --since "2026-04-30 00:00:00" --dry-run

# Incremental with CSV output for inspection
python main.py --since "2026-04-30 00:00:00" --output csv
```

**Log output for a typical incremental run:**

```
2026-04-30 06:00:01  INFO     s0_changed_users  [s0_changed_users] 1,247 users with new completions since 2026-04-29 00:00:00
2026-04-30 06:00:03  INFO     main              Incremental run: 1,247 users with new completions since 2026-04-29 00:00:00
2026-04-30 06:00:03  INFO     main              Scoped to 1,247 users matching both filters and new-completion list
2026-04-30 06:00:03  INFO     main              Large run: 1,247 users → 1 chunks of up to 2000 each
...
2026-04-30 06:07:14  INFO     main              Pipeline complete.
```

If there are no new completions since the given timestamp, the pipeline exits cleanly:

```
2026-04-30 06:00:01  INFO     main  Incremental run: no new completions since 2026-04-30 00:00:00 — nothing to update.
```

---

### Recommended Daily Schedule

Run incremental at midnight (or just after) each day, pointing `--since` at the previous midnight. This picks up all completions recorded during the day.

**Cron example (runs at 00:05 every day):**

```cron
5 0 * * * cd /path/to/ael_v2_pipeline && /path/to/venv/bin/python main.py \
  --since "$(date -v-1d '+%Y-%m-%d 00:00:00')" \
  >> /var/log/ael_pipeline.log 2>&1
```

> On Linux, replace `date -v-1d` with `date -d 'yesterday'`:
> ```cron
> --since "$(date -d 'yesterday' '+%Y-%m-%d 00:00:00')"
> ```

**Run the full refresh monthly** (or after any bulk allocation change) to ensure allocation data is fresh:

```cron
# Full refresh on the 1st of every month at 01:00
0 1 1 * * cd /path/to/ael_v2_pipeline && /path/to/venv/bin/python main.py \
  >> /var/log/ael_pipeline_full.log 2>&1
```

---

### First-Run Requirement

Incremental mode **requires a prior full refresh** to have already populated the output tables. On first deployment:

```bash
# 1. Full refresh — builds tables from scratch (~5 hours for 900K users)
python main.py

# 2. From the next day onwards — incremental daily runs (minutes)
python main.py --since "2026-04-30 00:00:00"
# Run every night at 2am, pick up everything completed since yesterday 2am
python main.py --since "$(date -d '24 hours ago' '+%Y-%m-%d %H:%M:%S')"

```

If the output tables are empty or do not exist, running with `--since` will still work correctly: `delete_user_rows()` silently no-ops on a missing table (catches MySQL error 1146), and `write_table()` auto-creates the table on first insert. However, only the changed users will be present — the table will be incomplete. Always start with a full refresh.

---

## Large Runs and Chunked Processing

Running the pipeline for the full user base (900K+ users) without any filters would require loading hundreds of millions of rows into memory at once, which causes the OS to kill the process. The pipeline avoids this through **chunked processing**.

### How Chunking Works

After Step 1 fetches the full user list (lightweight — just `users` + `student_details`), the pipeline splits the user IDs into batches of `ALLOC_CHUNK_SIZE` (default: 2,000) and processes each batch end-to-end:

```
for each chunk of 2,000 user_ids:
    Step 2: fetch allocation   (WHERE user_id IN (...2000 ids...))
    Step 3: fetch completion   (for allocated users in this chunk only)
          → merge → add stubs → write to DB → discard from memory
```

Each chunk peaks at roughly `2,000 users × ~300 lessons = ~600K rows` in memory before the result is written and discarded. The overall memory footprint stays bounded regardless of the total number of users.

**DB write strategy across chunks:**

| Run type | First chunk | Subsequent chunks |
|---|---|---|
| Full refresh | `TRUNCATE` + INSERT (replace) | INSERT only (append) |
| Incremental | DELETE user rows + INSERT (append) | DELETE user rows + INSERT (append) |

The `TRUNCATE` only happens once (first chunk of a full refresh). All subsequent chunks append to the already-truncated table.

### Configuration

`ALLOC_CHUNK_SIZE` is set in `config.py`:

```python
ALLOC_CHUNK_SIZE = 2000   # users per allocation query batch
```

Reduce this value if you encounter memory issues on the host machine. Increasing it reduces the number of round-trips but requires more RAM per chunk. 2,000 is the tested safe value for a standard 16GB machine.

### Debug CSV Limitation

The debug allocation CSV (`allocation_debug_*.csv`) is only written for single-chunk runs. On full runs (multiple chunks) the debug file is skipped — the per-chunk allocation DataFrames are discarded after the DB write and are not concatenated back into a single file.

---

## Database Reference

### Source Tables (`quest_rearch_production`)

| Table | Role in pipeline |
|---|---|
| `users` | User list — filtered by `type IN (3,4)`, `status=1`, `deleted_at IS NULL` |
| `student_details` | Provides `batch_id`, `trade_id` per user |
| `centre_subject` | Maps centre → subjects (base of allocation) |
| `batch_subject` | Maps batch → subjects |
| `subject_trade` | Maps trade → subjects (non-PLE) |
| `subject_ple_career_path` | Maps PLE career path → subjects |
| `ple_career_path_user` | Maps user → career path (`job_type_id`) |
| `ple_career_paths` | Career path master data (name, id) |
| `subjects` | Subject master — `status=1`, `deleted_at IS NULL` |
| `lessons` | Lesson master — `status=1`, `deleted_at IS NULL`, `student_access=1` |
| `lesson_types` | Lesson type labels (Video, PDF, Assessment, etc.) |
| `trades` | Trade master — provides `duration` (in years) for year_to_map filter |
| `learning_activities` | Lesson completion for user types 3 and 4 (learners / alumni) |
| `facilitator_learning_activities` | Lesson completion for all other user types |

### Analytics Tables (`quest_ple_analytics`)

| Table | Role in pipeline |
|---|---|
| `main_learning_activity_myquest_ael` | **Primary output** — subject-level aggregation (one row per user × subject). Written by this pipeline when `--output db` or `--output both` is used. |
| `main_learning_activity_myquest_ael_lesson` | **Lesson-level detail** — one row per user × completed lesson (plus zero-completion stub rows). |

---

## Understanding `toolkit_type`

The `toolkit_type` column classifies which user role the lesson is intended for. It is **derived** from access flags on the `lessons` table — it is not a stored column.

| `toolkit_type` value | Condition | Meaning |
|---|---|---|
| `student` | `lessons.student_access = 1` | Content for learners (types 3, 4) |
| `facilitator` | `lessons.facilitator_access = 1` | Content for facilitators / trainers |
| `master` | `lessons.mastertrainer_access = 1` | Content for master trainers |
| `NULL` | None of the above flags set | No access flag configured |

This pipeline filters `lessons` to `student_access = 1` only, so all rows in the output will have `toolkit_type = 'student'` unless a lesson has multiple access flags set simultaneously.

---

## SSH Tunnel Architecture

Both RDS instances are in private subnets accessible only through bastion (jump) servers. The pipeline establishes SSH tunnels using **paramiko** directly — no third-party `sshtunnel` library.

**Why not `sshtunnel`?** Version 0.4.0 references `paramiko.DSSKey` which was removed in paramiko 3.x, causing an `AttributeError` at import time.

**How the custom tunnel works (`db.py`):**

1. A `paramiko.SSHClient` connects to the bastion server using the `.pem` private key.
2. A local TCP server socket is bound to a free port on `127.0.0.1`.
3. A background thread (`_serve`) accepts TCP connections on that local port and opens a `direct-tcpip` channel through the SSH session to the RDS endpoint.
4. A second thread (`_bridge`) handles bidirectional data copying between the local socket and the paramiko channel.
5. `pymysql.connect()` connects to `127.0.0.1:<local_port>` as if it were connecting directly to RDS.
6. The tunnel tears down automatically when the context manager exits.

```
pymysql → 127.0.0.1:local_port → [paramiko direct-tcpip channel] → bastion → RDS
```

Each call to `fetch()` or `write_table()` opens and closes its own SSH tunnel. There is no persistent connection pool.

---

## Troubleshooting

**Duplicate lesson rows in the output**

This is caused by a user being enrolled in more than one active career path (`ple_career_path_user` has multiple rows with `status=1`). The PLE allocation query joins to every active career path, so lessons shared between two paths appear once per path. The pipeline deduplicates automatically in `fetch_allocation()` — the most recently updated career path (`pcpu.updated_at DESC`) wins. The log line `deduplicated N duplicate (user_id, lesson_id) rows` confirms how many were dropped. If you still see duplicates, check that `drop_duplicates(subset=["user_id", "lesson_id"])` is being reached (e.g. no early return on empty DataFrame).

---

**Empty allocation results for a user**

The most common causes:
1. Missing `student_details` row — check with:
   ```sql
   SELECT * FROM student_details WHERE user_id = '<user_id>';
   ```
2. `batch_id` or `trade_id` is NULL in `student_details`
3. The batch has no entries in `batch_subject` for the user's subjects
4. The trade has no entries in `subject_trade` for the user's subjects
5. For PLE users — no active record in `ple_career_path_user` (`status=1`, `deleted_at IS NULL`)
6. All the user's subjects are filtered out by the `year_to_map <= trade.duration` rule — check `subjects.year_to_map` and `trades.duration` for the user's trade

**Empty completion results (output is empty even though allocation is non-empty)**

The user has no records in `learning_activities` (or `facilitator_learning_activities`) for any of their allocated lessons. Since the final output is filtered to `completed = 1`, an empty result just means the user has not started any allocated lessons. Run with `--dry-run` to see the full summary including `total_allocated_lessons`.

**`AuthenticationException` on SSH connect**

- Check the `.pem` file exists in `DB_Config/` with the exact filename set in `.env`
- Set correct permissions: `chmod 400 DB_Config/joseph_prod.pem`
- Confirm the SSH username (`joseph_prod` for source, `ubuntu` for analytics) matches the key

**`No route to host` or `Connection timed out`**

You need to be on the correct network to reach the bastion hosts configured in `.env` (`SOURCE_SSH_HOST` and `DEST_SSH_HOST`). Check VPN or office network connectivity before running.

**`Could not open channel` warning in logs**

The paramiko channel to RDS failed after the SSH connection succeeded. Usually means the bastion does not have IP routing to the RDS endpoint — verify the `SOURCE_RDS_HOST` / `DEST_RDS_HOST` values in `.env`.

**`ModuleNotFoundError`**

The virtual environment is not activated or dependencies are not installed:
```bash
source venv/bin/activate
pip install -r requirements.txt
```

**`OperationalError: (1054, "Unknown column ...")`**

The SQL references a column that does not exist in the source table. The most common historical instance was `data_from` — this was fixed by replacing `MAX(data_from)` with `NULL AS data_from` in `s3_completion.py` since the source `learning_activities` table does not have that column.

**Large run takes a long time**

Full refresh for the entire user base (900K+ users) takes approximately 5 hours. Each chunk of 2,000 users requires two SSH tunnel round-trips (allocation + completion). There is no connection pooling — each fetch opens and closes its own tunnel.

For routine updates, use incremental mode (`--since`) — see [Incremental Runs](#incremental-runs). This typically completes in minutes by processing only users with new completions.

---

**Process killed (`Killed`) or out-of-memory error**

This happens when the pipeline tries to load too much data at once. The chunked processing (`ALLOC_CHUNK_SIZE = 2000`) was added specifically to prevent this. If you still see OOM on your host:

1. Reduce `ALLOC_CHUNK_SIZE` in `config.py` (try 500 or 1000)
2. Close other memory-intensive applications
3. Run with a scoping filter first (`--centre-id` or `--batch-id`) to validate the pipeline before a full run

---

**Incremental run produces no output even though completions occurred**

The `--since` timestamp must be in `YYYY-MM-DD HH:MM:SS` format and must be in UTC (or whatever timezone `learning_activities.completed_at` is stored in). If the timestamp is in the future or too recent, the query finds zero changed users and exits cleanly. Try widening the window:

```bash
python main.py --since "2026-04-01 00:00:00" --dry-run
```

---

**Incremental run re-inserts a user who did not change**

This is expected if the user's `completed_at` falls after the `--since` threshold even for lessons they completed before the last full refresh. The DELETE + re-insert approach is idempotent — re-inserting the same data is safe and produces the correct final state.

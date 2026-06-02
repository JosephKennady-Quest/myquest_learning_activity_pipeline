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
  - [Step 4 — User Project/Phase JSON](#step-4--user-projectphase-json-s4_users_project_phase_jsonpy)
- [Allocation Logic](#allocation-logic)
  - [Non-PLE Path](#non-ple-path)
  - [PLE Path](#ple-path)
  - [Staff Path](#staff-path)
  - [Optional Batch / Trade Intersection](#optional-batch--trade-intersection)
  - [Year-to-Map Filtering](#year-to-map-filtering)
  - [Subject Platform Filter](#subject-platform-filter-subjectsis_ple)
  - [Data Quality Rules](#data-quality-rules)
- [Completion Logic](#completion-logic)
  - [Source Table Routing](#source-table-routing)
  - [Completed = 1 Filter](#completed--1-filter)
- [Output Schema](#output-schema)
- [Running the Pipeline](#running-the-pipeline)
  - [All Users — Full Refresh](#all-users--full-refresh)
  - [Single User](#single-user)
  - [Filter Options](#filter-options)
  - [Output Options](#output-options)
  - [Controlling Which Outputs Are Written](#controlling-which-outputs-are-written)
  - [All Lesson Types](#all-lesson-types)
  - [Dry Run](#dry-run)
- [Incremental Runs](#incremental-runs)
- [Large Runs and Chunked Processing](#large-runs-and-chunked-processing)
- [Database Reference](#database-reference)
- [Understanding toolkit_type](#understanding-toolkit_type)
- [SSH Tunnel Architecture](#ssh-tunnel-architecture)
- [Troubleshooting](#troubleshooting)

---

## Overview

This pipeline answers the question:

> **For every active user — what toolkit content (subjects + lessons) are they allocated, and of that allocated content, how much have they completed?**

It handles **four user types** across **three allocation paths**:

| User type | Description | Allocation path | Lesson access |
|---|---|---|---|
| **3** | Learner | `non_ple` or `ple` | `student_access = 1` |
| **4** | Alumni | `non_ple` or `ple` | `student_access = 1` |
| **1** | Admin | `staff` | All lessons in centre |
| **2** | Facilitator / Master Trainer | `staff` | `facilitator_access = 1` or `mastertrainer_access = 1` |

All four paths are merged into a single output with an `allocation_path` column (`non_ple`, `ple`, `staff`) for traceability.

---

## Architecture

```
quest_rearch_production (source DB — read only)
        │
        ├── users + student_details            → Step 1: user list (types 1–4)
        │
        ├── centre_subject                     ┐
        ├── batch_subject                      │ Step 2: allocation
        ├── subject_trade                      │  non_ple path: centre ∩ batch ∩ trade
        ├── subject_ple_career_path            │  ple path:    centre ∩ career path ∩ batch
        ├── ple_career_path_user               │  staff path:  centre only
        ├── ple_career_paths                   │
        ├── subjects + lessons                 │
        ├── lesson_types                       │
        └── trades                             ┘  ← year_to_map duration filter
        │
        ├── learning_activities                ┐ Step 3: completion
        └── facilitator_learning_activities    ┘ (routed by user_type)

quest_analytics (analytics DB — write only)
        ├── main_learning_activity_myquest_ael                ← subject-level, filtered         ┐ Steps 1–3
        ├── main_learning_activity_myquest_ael_all_lesson_type ← subject-level, all types       │
        ├── main_learning_activity_myquest_ael_lesson         ← lesson-level, filtered          ┘
        │
        └── main_wcc_json                                     ← one row per user (Step 4)
                ├── project_phase_combos  JSON  ← from main_centre_project + main_phases
                └── subject_combos        JSON  ← from main_learning_activity_myquest_ael

                        ↓
              pandas merge (LEFT JOIN allocation × completion)
                        ↓
              filter: completed = 1 only
                        ↓
              ┌─────────────────────┐
              │  Final Output       │
              │  (DB tables / CSV)  │
              └─────────────────────┘
```

---

## Prerequisites

- Python 3.11+
- SSH access to both bastion hosts (`.pem` key files):
  - `<source-key>.pem` — source DB bastion
  - `<dest-key>.pem` — analytics DB bastion
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

The `.pem` key filenames are resolved against `DB_Config/` inside the pipeline folder.

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
│                         # Constants: LEARNER_TYPES (3,4), STAFF_TYPES (1,2),
│                         #   ALL_TYPES (1,2,3,4), CHUNK_SIZE, ALLOC_CHUNK_SIZE (2000),
│                         #   STAFF_ALLOC_CHUNK_SIZE (200), OUTPUT_DIR
│
├── db.py                 # Low-level DB helpers (SSH tunnel + MySQL):
│                         #   fetch()            — SELECT query → DataFrame
│                         #   write_table()      — DataFrame → MySQL table (chunked)
│                         #   delete_user_rows() — removes rows for specific users
│                         # Tunnel: custom local port forwarder (paramiko direct-tcpip)
│
├── steps/
│   ├── __init__.py
│   │
│   ├── s0_changed_users.py  # Incremental mode: finds user_ids with new completions
│   │                        # since a given timestamp (queries learning_activities.completed_at)
│   │
│   ├── s1_users.py       # Fetch active users ALL types (1–4) with student_details profile
│   │                     # Staff (types 1,2) have NULL for student_details columns
│   │
│   ├── s2_allocation.py  # Core allocation logic — three paths:
│   │                     #   fetch_non_ple_allocation()   — learner/alumni non-PLE
│   │                     #   fetch_ple_allocation()       — learner/alumni PLE
│   │                     #   fetch_staff_allocation()     — Admin + Facilitator/MT
│   │                     #   fetch_allocation(paths=...)  — combined, deduplicated
│   │                     # Applies year_to_map <= trade.duration filter (learners)
│   │                     # Optional batch/trade intersection (LEFT JOIN + NULL guards)
│   │
│   ├── s3_completion.py  # Completion data from source DB:
│   │                     #   fetch_student_completion()     — learning_activities
│   │                     #   fetch_facilitator_completion() — facilitator_learning_activities
│   │                     #   fetch_completion()             — auto-routes by user_type
│   │                     #   merge_completion()             — LEFT JOIN + summary + filter
│   │
│   └── s4_users_project_phase_json.py
│                         # One-row-per-user JSON builder (analytics DB only):
│                         #   fetch_users_project_phase()    — main_users LEFT JOIN main_centre_project + main_phases
│                         #   fetch_subjects()               — main_learning_activity_myquest_ael
│                         #   build_users_project_phase_json() — collapse to project_phase_combos JSON per user
│                         #   build_subject_json()           — collapse to subject_combos JSON per user
│                         #   run_users_project_phase_json() — orchestrates all four, returns merged DataFrame
│
├── main.py               # CLI entry point / orchestrator (Steps 1–3)
├── main_wcc_json_v2.py   # CLI entry point for Step 4 (JSON output)
│                         # Args: --user-id, --centre-id, --batch-id, --subject-id,
│                         #       --trade-id, --output db(default)/csv/both,
│                         #       --outputs, --all-lesson-types, --since, --dry-run,
│                         #       --log-file
│
├── DB_Config/            # SSH private key files (.pem) — NOT committed
└── output/               # Default CSV output directory (only with --output csv/both)
```

---

## Pipeline Steps

### Step 1 — User Fetch (`s1_users.py`)

Fetches all active users across all four user types with their student profile.

**Source tables:** `users` LEFT JOIN `student_details`

**Filters applied:**
- `users.type IN (1, 2, 3, 4)` — Admin, Facilitator/Master Trainer, Learner, Alumni
- `users.status = 1` — active accounts only
- `users.deleted_at IS NULL` — non-deleted only

**Key fields returned:**

| Field | Source | Notes |
|---|---|---|
| `user_id` | `users.id` | Primary identifier (UUID) |
| `user_type` | `users.type` | 1=Admin, 2=Facilitator/MT, 3=Learner, 4=Alumni |
| `is_master_trainer` | `users.is_master_trainer` | Distinguishes Facilitator vs Master Trainer (type 2) |
| `centre_id` | `users.centre_id` | Drives subject allocation |
| `is_ple` | `users.is_ple` | Routes learners/alumni to PLE or non-PLE path |
| `batch_id` | `student_details.batch_id` | NULL for staff — not used in staff allocation |
| `trade_id` | `student_details.trade_id` | NULL for staff and PLE users |

> Staff users (types 1, 2) will have NULL for all `student_details` columns — this is expected. Their allocation is driven by `centre_id` only.

---

### Step 2 — Toolkit Allocation (`s2_allocation.py`)

Builds the full list of subjects and lessons each user is allocated. Three independent allocation paths are run, then combined.

```python
fetch_allocation(
    user_ids=None,               # list of UUIDs — used by chunked loop
    centre_id=None, batch_id=None,
    subject_id=None, trade_id=None,
    paths=("non_ple", "ple", "staff"),  # which paths to run
)
```

The `paths` parameter lets each chunk run only the relevant subset (learner chunks skip `staff`, staff chunks skip `non_ple` and `ple`).

See [Allocation Logic](#allocation-logic) for full join chains and filtering rules.

---

### Step 3 — Completion & Merge (`s3_completion.py`)

Fetches raw lesson activity data and LEFT JOINs it onto the allocation result.

```python
fetch_completion(user_ids, user_types={3,4})  # auto-routes by type; user_ids required
merge_completion(allocation_df, completion_df) # LEFT JOIN + summary stats + stubs
```

**Source table routing:**

| User type | Source table |
|---|---|
| 3, 4 (Learner, Alumni) | `learning_activities` |
| 1, 2 (Admin, Facilitator/MT) | `facilitator_learning_activities` |

`main.py` extracts the unique `user_type` values from the allocation DataFrame and passes them to `fetch_completion()` — routing is automatic and transparent.

---

### Step 4 — User Project/Phase JSON (`s4_users_project_phase_json.py`)

Builds a **one-row-per-user** JSON output table (`quest_analytics.main_wcc_json`) that consolidates project, phase, and subject data per user. Because a single user can belong to multiple projects and phases, repeating values are collapsed into two JSON columns instead of producing multiple rows.

**Source tables (all from `quest_analytics`):**

| Table | Role |
|---|---|
| `main_users` | Base user list — one row per user, drives the output shape |
| `main_centre_project` | Maps centre → project (LEFT JOIN on `centre_id`) |
| `main_phases` | Maps batch + centre + project + user → phase (LEFT JOIN, inner join on `p_user_id`) |
| `main_learning_activity_myquest_ael` | Subject-level completion data for `subject_combos` JSON |

**Join logic:**

```
main_users u
  LEFT JOIN main_centre_project cp  ON cp.centre_id   = u.centre_id
  LEFT JOIN main_phases ph          ON ph.p_batch_id   = u.batch_id
                                    AND ph.p_centre_id  = u.centre_id
                                    AND ph.p_project_id = cp.project_id
                                    AND ph.p_user_id    = u.id
```

**Output JSON columns:**

`project_phase_combos` — one entry per unique project/phase combination the user belongs to:

| Field | Source |
|---|---|
| `prog_name` | `main_centre_project.program_name` |
| `project_id` | `main_centre_project.project_id` |
| `proj_name` | `main_centre_project.project_name` |
| `p_phase_id` | `main_phases.p_phase_id` |
| `phase` | `main_phases.phase_name` |

`subject_combos` — one entry per subject allocated and/or completed by the user:

| Field | Source |
|---|---|
| `sub_id` | `subject_id` |
| `sub_name` | `subject_name` |
| `avg_score_a` | `avg_score` |
| `avg_rating_a` | `avg_rating` |
| `c_sub_w_less_asse_c` | `subj_total_completed` |
| `a_sub_w_less_asse_c` | `subj_total_allocated` |
| `a_sub_w_assess_c` | `subj_assessments_allocated` |
| `a_sub_w_lesson_c` | `subj_lessons_allocated` |
| `c_sub_w_assess_c` | `subj_assessments_completed` |
| `c_sub_w_less_c` | `subj_lessons_completed` |
| `year_category` | `year_to_map` |

**Key functions:**

| Function | Description |
|---|---|
| `fetch_users_project_phase()` | Fetches the joined user + project/phase rows from analytics DB |
| `fetch_subjects()` | Fetches subject-level rows from `main_learning_activity_myquest_ael` |
| `build_users_project_phase_json()` | Collapses multi-row join result into one row per user with `project_phase_combos` JSON |
| `build_subject_json()` | Collapses subject rows into `subject_combos` JSON per user |
| `run_users_project_phase_json()` | Orchestrates all four functions and merges the result |

All filters (`--user-id`, `--centre-id`, `--batch-id`) are passed through to both fetch functions so scoped runs work correctly.

**Running step 4 via `main_wcc_json_v2.py`:**

```bash
# Full refresh into quest_analytics.main_wcc_json
python main_wcc_json_v2.py

# Dry run only
python main_wcc_json_v2.py --dry-run

# Test one user, write to CSV
python main_wcc_json_v2.py --user-id <tlo_user_id> --output csv

# Write both CSV and DB
python main_wcc_json_v2.py --output both
```

**Querying the JSON output in MySQL:**

```sql
-- Find all users in a specific project and phase
SELECT DISTINCT u.*
FROM quest_analytics.main_wcc_json u
JOIN JSON_TABLE(
    u.project_phase_combos,
    '$[*]' COLUMNS (
        proj_name VARCHAR(255) PATH '$.proj_name',
        phase     VARCHAR(255) PATH '$.phase'
    )
) jt
WHERE jt.proj_name = 'Project A'
  AND jt.phase = 'Phase 1';

-- Find all users with a specific subject
SELECT DISTINCT u.tlo_user_id, u.centre_name
FROM quest_analytics.main_wcc_json u
JOIN JSON_TABLE(
    u.subject_combos,
    '$[*]' COLUMNS (
        sub_name VARCHAR(255) PATH '$.sub_name'
    )
) jt
WHERE jt.sub_name = 'Digital Literacy';
```

> The JSON structure follows the same field names used in `Cust JSON version/json_main_wcc_try.ipynb` for consistency with the existing WCC JSON pipeline.

---

## Allocation Logic

### Non-PLE Path

For learners/alumni (`users.type IN (3,4)`) where `users.is_ple != 1`.

A subject is allocated if it appears at the **intersection** of the user's centre, batch, and trade mappings. Batch and trade are **optional** — if the user has no `batch_id` or `trade_id`, the corresponding intersection check is skipped and the centre subjects pass through.

```
centre_subject   (always applied — base set)
    ∩  (only if batch_id is not NULL)
batch_subject
    ∩  (only if trade_id is not NULL)
subject_trade
```

**Full join chain (simplified):**

```sql
users
  LEFT JOIN student_details sd   ON sd.user_id = u.id
  JOIN centre_subject cs         ON cs.centre_id = u.centre_id
  LEFT JOIN batch_subject bs
      ON  sd.batch_id IS NOT NULL
      AND bs.batch_id   = sd.batch_id
      AND bs.subject_id = cs.subject_id
  LEFT JOIN subject_trade st
      ON  sd.trade_id IS NOT NULL
      AND st.trade_id   = sd.trade_id
      AND st.subject_id = cs.subject_id
  LEFT JOIN trades t_trade       ON t_trade.id = sd.trade_id
  JOIN subjects s                ON s.id = cs.subject_id
  JOIN lessons l                 ON l.subject_id = s.id
                                    AND l.student_access = 1
  LEFT JOIN lesson_types lt      ON lt.id = l.lesson_type_id

WHERE u.type IN (3, 4)
  AND u.status = 1 AND u.deleted_at IS NULL
  AND (u.is_ple IS NULL OR u.is_ple != 1)
  AND s.is_ple IN (0, 2)
  AND (sd.batch_id IS NULL OR bs.subject_id IS NOT NULL)
  AND (sd.trade_id IS NULL OR st.subject_id IS NOT NULL)
  AND year_to_map filter...
```

**Intersection examples:**

| User has | What is checked |
|---|---|
| centre_id only | Centre subjects only |
| centre_id + batch_id | Centre ∩ Batch |
| centre_id + trade_id | Centre ∩ Trade |
| centre_id + batch_id + trade_id | Centre ∩ Batch ∩ Trade |

---

### PLE Path

For learners/alumni (`users.type IN (3,4)`) where `users.is_ple = 1`.

```
centre_subject   (always applied)
    ∩  (only if career path exists)
subject_ple_career_path
    ∩  (only if batch_id is not NULL)
batch_subject
```

Career path is resolved via:
```
ple_career_path_user (user_id = users.id, status=1, deleted_at IS NULL, latest by updated_at)
    → ple_career_paths (id = job_type_id, deleted_at IS NULL)
```

Only the **most recently updated** active career path is joined (enforced via `ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY updated_at DESC) = 1`).

---

### Staff Path

For Admin (type 1) and Facilitator/Master Trainer (type 2). No batch, trade, or career path joins — allocation is driven by **centre subjects only**.

```sql
users u
  JOIN centre_subject cs ON cs.centre_id = u.centre_id
  JOIN subjects s        ON s.id = cs.subject_id AND s.status = 1
  JOIN lessons l         ON l.subject_id = s.id AND l.status = 1
  LEFT JOIN lesson_types lt ON lt.id = l.lesson_type_id

WHERE u.type IN (1, 2)
  AND u.status = 1 AND u.deleted_at IS NULL
  AND s.is_ple IN (0, 1, 2)
  AND (
      u.type = 1                                                         -- Admin: all lessons
      OR (u.type = 2 AND (u.is_master_trainer IS NULL OR u.is_master_trainer != 1)
          AND l.facilitator_access = 1)                                  -- Facilitator
      OR (u.type = 2 AND u.is_master_trainer = 1
          AND l.mastertrainer_access = 1)                                -- Master Trainer
  )
```

| Staff role | `is_master_trainer` | Lesson access filter |
|---|---|---|
| Admin (type 1) | any | All lessons in the centre |
| Facilitator (type 2) | NULL or 0 | `facilitator_access = 1` |
| Master Trainer (type 2) | 1 | `mastertrainer_access = 1` |

Staff have access to subjects regardless of `subjects.is_ple` — they can see both QuestApp and MyQuest subjects in their centre.

---

### Optional Batch / Trade Intersection

The NULL guards on the JOIN ON clauses ensure that MySQL skips scanning the mapping tables when the key is NULL — making the optional intersection zero-cost rather than causing a full table scan:

```sql
-- Only join batch_subject when the user actually has a batch_id
LEFT JOIN batch_subject bs
    ON  sd.batch_id IS NOT NULL          -- MySQL skips scan when NULL
    AND bs.batch_id   = sd.batch_id
    AND bs.subject_id = cs.subject_id

-- Then in WHERE: include the subject if there was no batch_id to check
AND (sd.batch_id IS NULL OR bs.subject_id IS NOT NULL)
```

---

### Year-to-Map Filtering

`subjects.year_to_map` restricts which year of a multi-year trade programme a subject belongs to. Applies to learner/alumni paths only (staff have no trade).

| Trade duration | Subjects included |
|---|---|
| 1 year | Only `year_to_map = 1` (or NULL / 0) |
| 2 years | `year_to_map = 1` **and** `year_to_map = 2` |

**Pass-through** (subject always included): `year_to_map IS NULL`, `year_to_map = 0`, `trades.duration IS NULL`.

---

### Subject Platform Filter (`subjects.is_ple`)

| `subjects.is_ple` | Platform | Allocated to |
|---|---|---|
| `0` | QuestApp | Non-PLE users only |
| `1` | MyQuest | PLE users only |
| `2` | Both | All users |

- Non-PLE query: `AND s.is_ple IN (0, 2)`
- PLE query: `AND s.is_ple IN (1, 2)`
- Staff query: `AND s.is_ple IN (0, 1, 2)` — all platforms

---

### Data Quality Rules

| Entity | Rule |
|---|---|
| `users` | `status = 1` AND `deleted_at IS NULL` AND `type IN (1, 2, 3, 4)` |
| `subjects` | `status = 1` AND `deleted_at IS NULL` |
| `lessons` | `status = 1` AND `deleted_at IS NULL` AND access flag per user type |
| `ple_career_paths` | `deleted_at IS NULL` |
| `ple_career_path_user` | `status = 1` AND `deleted_at IS NULL` |

**Multi-career-path deduplication:** PLE users with more than one active career path produce one row per career path per shared lesson. `fetch_allocation()` deduplicates on `(user_id, lesson_id)`, keeping the **most recently updated** career path row. The `career_path_updated_at` helper column is dropped after dedup.

---

## Completion Logic

### Source Table Routing

| User type | Source table |
|---|---|
| 3 (Learner), 4 (Alumni) | `learning_activities` |
| 1 (Admin), 2 (Facilitator/MT) | `facilitator_learning_activities` |

Per `(user_id, lesson_id)` the query computes: `MAX(score)`, `MAX(rating)`, `SUM(duration)`. `data_from` is always `NULL AS data_from` (column does not exist in the source tables).

### Completed = 1 Filter

Enforced at two layers:

**Layer 1 — SQL:** `WHERE completed = 1` in `s3_completion.py`. Only records explicitly marked completed are fetched. Viewed-only or in-progress records are excluded at the DB level.

**Layer 2 — Python:** `merge_completion()` LEFT JOINs completion onto allocation. Lessons in the allocation with no `completed = 1` record get `completed = 0`. Summary stats are computed on the **full merged dataset** before this filter, so `total_allocated`, `total_completed`, and `completion_pct` are accurate. The final returned DataFrame contains only `completed = 1` rows plus zero-completion stub rows.

---

## Output Schema

### DB Tables Written (every run)

| Table | Contents | Lesson type filter |
|---|---|---|
| `main_learning_activity_myquest_ael_lesson` | Lesson-level (one row per user × completed lesson) | pdf / mp4 / pdf web **excluded** |
| `main_learning_activity_myquest_ael` | Subject-level aggregation (one row per user × subject) | pdf / mp4 / pdf web **excluded** |
| `main_learning_activity_myquest_ael_all_lesson_type` | Subject-level aggregation | **All lesson types included** |

The first two tables are written per-chunk during the run. The all-lesson-type table is written once after all chunks complete.

---

### Lesson-Level Output (`main_learning_activity_myquest_ael_lesson`)

One row per **user × completed lesson**. Users with zero completions get a single stub row with lesson/subject fields NULL and `completed = 0`.

| Column | Type | Description |
|---|---|---|
| `user_id` | string | User UUID |
| `user_name` | string | Full name |
| `user_type` | int | 1=Admin, 2=Facilitator/MT, 3=Learner, 4=Alumni |
| `is_master_trainer` | int | 1 = Master Trainer (type 2 only) |
| `centre_id` | string | Centre UUID |
| `project_id` | string | Project UUID |
| `batch_id` | string | Batch UUID (NULL for staff) |
| `trade_id` | string | Trade UUID (non-PLE only; NULL for PLE and staff) |
| `career_path_id` | string | PLE career path UUID (PLE only) |
| `career_path_name` | string | PLE career path name (PLE only) |
| `subject_id` | string | Subject UUID |
| `subject_name` | string | Subject name |
| `subject_is_ple` | int | PLE flag on the subject (0 / 1 / 2) |
| `ple_career_path_id` | string | Career path linked directly on the subject |
| `year_to_map` | int | Year restriction on the subject (NULL = unrestricted) |
| `trade_duration` | int | Duration of the user's trade (NULL for staff) |
| `subject_order` | int | Display order from `centre_subject` |
| `lesson_id` | string | Lesson UUID |
| `lesson_name` | string | Lesson name |
| `lesson_order` | int | Display order within the subject |
| `lesson_type` | string | e.g. Video, PDF, Assessment |
| `is_assessment` | int | 1 = assessment, 0 = learning content |
| `toolkit_type` | string | `student` / `facilitator` / `master` |
| `allocation_path` | string | `non_ple` / `ple` / `staff` |
| `allocation_basis` | string | Which mapping tables allocated this row |
| `score` | float | Best score (NULL if not attempted) |
| `rating` | float | Best rating (NULL if not rated) |
| `duration` | int | Total time on this lesson across completed attempts (seconds) |
| `data_from` | string | Always NULL — not in source tables |
| `completed` | int | `1` = completed; `0` = zero-completion stub |
| `total_allocated` | int | All lessons allocated to this user |
| `total_lessons_allocated` | int | Non-assessment lessons allocated |
| `total_assessments_allocated` | int | Assessment lessons allocated |
| `total_completed` | int | All lessons completed by this user |
| `total_lessons_completed` | int | Non-assessment lessons completed |
| `total_assessments_completed` | int | Assessment lessons completed |
| `completion_pct` | float | `(total_completed / total_allocated) × 100`, 2dp |
| `subj_total_allocated` | int | Total lessons allocated in this subject |
| `subj_lessons_allocated` | int | Non-assessment lessons allocated in this subject |
| `subj_assessments_allocated` | int | Assessment lessons allocated in this subject |
| `subj_total_completed` | int | Lessons completed in this subject |
| `subj_lessons_completed` | int | Non-assessment lessons completed in this subject |
| `subj_assessments_completed` | int | Assessment lessons completed in this subject |

> **`is_assessment` detection:** A lesson is classified as an assessment if `lessons.is_assessment = 1` **OR** `UPPER(lesson_name) LIKE '%ASSESSMENT%'`. This catches lessons where the DB flag was never set.

> **Zero-completion stub rows:** Users allocated lessons but with no completed records appear as a single stub row. All lesson/subject columns are NULL. User-level stats are filled in with `completed = 0`.

---

### Subject-Level Aggregation (`main_learning_activity_myquest_ael`)

One row per **user × subject**. Filtered — pdf / mp4 / pdf web lesson types excluded.

| Column | Type | Description |
|---|---|---|
| `user_id` | string | User UUID |
| `user_name` | string | Full name |
| `user_type` | int | 1=Admin, 2=Facilitator/MT, 3=Learner, 4=Alumni |
| `centre_id` | string | Centre UUID |
| `project_id` | string | Project UUID |
| `batch_id` | string | Batch UUID (NULL for staff) |
| `trade_id` | string | Trade UUID (non-PLE only) |
| `career_path_id` | string | PLE career path UUID |
| `career_path_name` | string | PLE career path name |
| `subject_id` | string | Subject UUID |
| `subject_name` | string | Subject name |
| `subject_is_ple` | int | PLE flag (0 / 1 / 2) |
| `year_to_map` | int | Year restriction on the subject |
| `allocation_basis` | string | Which mapping tables allocated this subject |
| `total_allocated` | int | All lessons allocated to this user |
| `total_lessons_allocated` | int | Non-assessment lessons allocated |
| `total_assessments_allocated` | int | Assessment lessons allocated |
| `total_completed` | int | All lessons completed |
| `total_lessons_completed` | int | Non-assessment lessons completed |
| `total_assessments_completed` | int | Assessment lessons completed |
| `completion_pct` | float | Completion percentage, 2dp |
| `subj_total_allocated` | int | Lessons allocated in this subject |
| `subj_lessons_allocated` | int | Non-assessment lessons in this subject |
| `subj_assessments_allocated` | int | Assessment lessons in this subject |
| `subj_total_completed` | int | Lessons completed in this subject |
| `subj_lessons_completed` | int | Non-assessment lessons completed |
| `subj_assessments_completed` | int | Assessment lessons completed |
| `avg_score` | float | Average score across completed lessons in this subject |
| `avg_rating` | float | Average rating across completed lessons |
| `avg_duration` | float | Average time per completed lesson (seconds) |
| `total_duration` | int | Total time across all completed lessons (seconds) |

---

### All-Lesson-Type Subject Aggregation (`main_learning_activity_myquest_ael_all_lesson_type`)

Same schema as `main_learning_activity_myquest_ael` above, but **includes pdf, mp4, and pdf web lesson types** in all counts and averages. Always written on every run alongside the filtered table.

---

## Running the Pipeline

### All Users — Full Refresh

```bash
cd ael_v2_pipeline
source venv/bin/activate
python main.py
```

Processes all active users (types 1–4). Writes to analytics DB (default). On first chunk it drops and recreates both filtered tables, then each subsequent chunk appends.

**When to run a full refresh:**

| Situation | Action |
|---|---|
| First time running | Full refresh |
| Allocation mappings changed (batch, trade, career path) | Full refresh |
| A subject or lesson was added/removed | Full refresh |
| Tables corrupted or out of sync | Full refresh |
| Routine daily update (completion data only changed) | Incremental `--since` — much faster |

> Full refresh for 900K+ users takes ~5 hours. Run during off-hours to avoid incomplete data in dashboards during the rebuild window.

---

### Single User

```bash
python main.py --user-id <user-uuid> --output csv
```

---

### Filter Options

All filters are optional and can be freely combined:

| Flag | Filters on |
|---|---|
| `--user-id <uuid>` | Single user |
| `--centre-id <uuid>` | All users in a centre |
| `--batch-id <uuid>` | All users in a batch |
| `--subject-id <uuid>` | One specific subject only |
| `--trade-id <uuid>` | Users in a specific trade |

The startup log line shows all active filters:
```
[user_id=ALL | centre_id=0dd48495-... | batch_id=ALL | subject_id=ALL | trade_id=ALL | output=db | outputs=lesson,subject,debug | dry_run=False]
```

---

### Output Options

| Flag | Behaviour |
|---|---|
| *(default)* / `--output db` | Write to analytics DB tables only |
| `--output csv` | Write CSV files to `OUTPUT_DIR/` only |
| `--output both` | Write both DB tables and CSV files |

**DB tables written on every run (when `--output db` or `--output both`):**

| Table | Contents |
|---|---|
| `main_learning_activity_myquest_ael_lesson` | Lesson-level, filtered |
| `main_learning_activity_myquest_ael` | Subject-level, filtered |
| `main_learning_activity_myquest_ael_all_lesson_type` | Subject-level, all lesson types |

**CSV files written (when `--output csv` or `--output both`):**

| File | Contents |
|---|---|
| `lessons_filtered_<tag>_<ts>.csv` | Lesson-level, pdf/mp4/pdf web excluded |
| `subjects_filtered_<tag>_<ts>.csv` | Subject-level, pdf/mp4/pdf web excluded |
| `no_allocation_users_<ts>.csv` | Users with no allocation found |

The `<tag>` reflects active filters: e.g. `ctr_0dd48495` for a centre run, `all_users` for a full run.

**Common examples:**

```bash
python main.py                                               # all users → DB (full refresh)
python main.py --output csv                                  # all users → CSV only
python main.py --output both                                 # all users → CSV + DB
python main.py --user-id <uuid> --output csv                 # single user to CSV
python main.py --centre-id <uuid>                            # centre → DB
python main.py --centre-id <uuid> --batch-id <uuid>          # combined filter
python main.py --trade-id <uuid>                             # all users in a trade
python main.py --subject-id <uuid>                           # all users, one subject
python main.py --since "2026-04-30 00:00:00"                 # incremental mode
python main.py --log-file /home/joseph/logs/ael.log          # also log to file
```

---

### Controlling Which Outputs Are Written

```bash
# Default: write lesson detail, subject aggregation, and debug CSV
python main.py --outputs lesson,subject,debug

# Skip debug output
python main.py --outputs lesson,subject

# Only write the subject-level aggregation
python main.py --outputs subject
```

The `debug` output writes a `debug_alloc_<tag>_<ts>.csv` showing the raw pre-completion allocation (with `allocation_basis`, `lesson_type`, etc.) for inspection. Only written on small runs (single chunk) when `--output csv` or `--output both` is active.

---

### All Lesson Types

By default, pdf, mp4, and pdf web lessons are **excluded** from all CSV outputs and from `main_learning_activity_myquest_ael` / `main_learning_activity_myquest_ael_lesson`. The all-lesson-type DB table (`main_learning_activity_myquest_ael_all_lesson_type`) is **always written** regardless.

To also get CSV files that include pdf/mp4/pdf web:

```bash
python main.py --output csv --all-lesson-types
# or
python main.py --output both --all-lesson-types
```

This adds two extra CSV files:

| File | Contents |
|---|---|
| `lessons_all_types_<tag>_<ts>.csv` | Lesson-level, all lesson types |
| `subjects_all_types_<tag>_<ts>.csv` | Subject-level, all lesson types |

---

### Dry Run

```bash
python main.py --dry-run
python main.py --centre-id <uuid> --dry-run
```

Prints row counts and a summary without writing any output.

**Sample output (scoped run):**

```
────────────────────────────────────────────────────────────
  Users processed       : 4,821  (PLE: 312 | non-PLE: 4,509)
  Unique subjects       : 148
  Unique lessons        : 2,034
  Total rows            : 97,210
  Avg completion        : 43.7%
────────────────────────────────────────────────────────────
```

**Sample output (full multi-chunk run):**

```
────────────────────────────────────────────────────────────
  Users fetched         : 931,035
  Users in output       : 931,035  (PLE: 12,480 | non-PLE: 903,291 | no-alloc: 15,264)
  Avg completion        : 31.4%
────────────────────────────────────────────────────────────
```

---

### Compressed Log File

```bash
python main.py --log-file /home/joseph/logs/ael_pipeline.log
```

Writes logs to a file in addition to stdout. Rotates daily at midnight; old files are gzip-compressed automatically. Kept for 30 days.

---

## Incremental Runs

After an initial full refresh, use `--since` to process only users with new completions. Typically completes in minutes.

```bash
python main.py --since "YYYY-MM-DD HH:MM:SS"
```

Step 0 queries `learning_activities.completed_at` to find users with new completions. Only those users are fetched, allocated, and re-inserted. All other users keep their existing DB rows untouched.

**Write strategy for incremental:**
1. `DELETE FROM <table> WHERE user_id IN (...)` — remove stale rows
2. `INSERT` fresh rows for changed users

**When to use:**

| Scenario | Mode |
|---|---|
| First deployment | Full refresh |
| Daily scheduled update | Incremental |
| Allocation mappings changed | Full refresh |
| Output tables dropped/corrupted | Full refresh |

**Cron example (runs at 00:05 daily):**

```cron
5 0 * * * cd /path/to/ael_v2_pipeline && /path/to/venv/bin/python main.py \
  --since "$(date -d 'yesterday' '+%Y-%m-%d 00:00:00')" \
  --log-file /var/log/ael_pipeline.log
```

> macOS: replace `date -d 'yesterday'` with `date -v-1d`.

**Monthly full refresh:**

```cron
0 1 1 * * cd /path/to/ael_v2_pipeline && /path/to/venv/bin/python main.py \
  --log-file /var/log/ael_pipeline_full.log
```

---

## Large Runs and Chunked Processing

Users are split into chunks and processed end-to-end per chunk to keep memory usage bounded. **Learners and staff use separate chunk sizes** because staff (especially Admins) produce many more rows per user.

```
After Step 1 — split into learner_ids (types 3,4) and staff_ids (types 1,2):

  for each learner chunk of ALLOC_CHUNK_SIZE (2000) users:
      Step 2: fetch_allocation(paths=("non_ple", "ple"))  — 2 queries
      Step 3: fetch completion → merge → write to DB → discard

  for each staff chunk of STAFF_ALLOC_CHUNK_SIZE (200) users:
      Step 2: fetch_allocation(paths=("staff",))           — 1 query
      Step 3: fetch completion → merge → write to DB → discard

After loop:
  Combine all unfiltered alloc frames → merge_completion → write to
  main_learning_activity_myquest_ael_all_lesson_type
```

The separation means Admin-heavy chunks never inflate the learner query sizes, restoring the original 2-queries-per-chunk cadence for the learner path.

**Configuration in `config.py`:**

```python
ALLOC_CHUNK_SIZE       = 2000   # learners per allocation query
STAFF_ALLOC_CHUNK_SIZE = 200    # staff per allocation query (admins get many more rows)
```

**DB write strategy:**

| Run type | First chunk | Subsequent chunks |
|---|---|---|
| Full refresh | DROP + CREATE + INSERT | INSERT (append) |
| Incremental | DELETE user rows + INSERT | DELETE user rows + INSERT |

---

## Database Reference

### Source Tables (`quest_rearch_production`)

| Table | Role |
|---|---|
| `users` | User list — types 1–4, `status=1`, `deleted_at IS NULL` |
| `student_details` | `batch_id`, `trade_id`, `educational_qualification_id`, etc. per learner |
| `centre_subject` | Maps centre → subjects |
| `batch_subject` | Maps batch → subjects |
| `subject_trade` | Maps trade → subjects (non-PLE) |
| `subject_ple_career_path` | Maps PLE career path → subjects |
| `ple_career_path_user` | Maps user → career path (`job_type_id`) |
| `ple_career_paths` | Career path master data |
| `subjects` | Subject master |
| `lessons` | Lesson master (`student_access`, `facilitator_access`, `mastertrainer_access`) |
| `lesson_types` | Lesson type labels (Video, PDF, Assessment, etc.) |
| `trades` | Trade master — `duration` (years) for year_to_map filter |
| `learning_activities` | Completion for user types 3, 4 |
| `facilitator_learning_activities` | Completion for user types 1, 2 |

### Analytics Tables (`quest_analytics`)

| Table | Role | Written by |
|---|---|---|
| `main_learning_activity_myquest_ael` | Subject-level, filtered (excludes pdf/mp4/pdf web) | `main.py` (Steps 1–3) |
| `main_learning_activity_myquest_ael_all_lesson_type` | Subject-level, all lesson types | `main.py` (Steps 1–3) |
| `main_learning_activity_myquest_ael_lesson` | Lesson-level detail, filtered | `main.py` (Steps 1–3) |
| `main_wcc_json` | One row per user — project/phase and subject data as JSON | `main_wcc_json_v2.py` (Step 4) |

---

## Understanding `toolkit_type`

Derived from access flags on the `lessons` table — not a stored column.

| Value | Condition | Meaning |
|---|---|---|
| `student` | `lessons.student_access = 1` | Content for learners/alumni |
| `facilitator` | `lessons.facilitator_access = 1` | Content for facilitators |
| `master` | `lessons.mastertrainer_access = 1` | Content for master trainers |
| `NULL` | None of the above set | No access flag configured |

Learner/alumni rows will always have `toolkit_type = 'student'`. Staff rows will have `facilitator` or `master`.

---

## SSH Tunnel Architecture

Both RDS instances are in private subnets accessible only through bastion hosts. The pipeline uses **paramiko** directly — no `sshtunnel` library.

**Why not `sshtunnel`?** Version 0.4.0 references `paramiko.DSSKey` which was removed in paramiko 3.x.

**How the tunnel works (`db.py`):**

1. `paramiko.SSHClient` connects to the bastion server using the `.pem` private key.
2. A local TCP socket binds to a free port on `127.0.0.1`.
3. A background thread accepts TCP connections and bridges them through a `direct-tcpip` paramiko channel to the RDS endpoint.
4. `pymysql.connect(host="127.0.0.1", port=<local_port>)` connects through the tunnel.
5. Tunnel tears down when the context manager exits.

```
pymysql → 127.0.0.1:local_port → [paramiko direct-tcpip] → bastion → RDS
```

Each `fetch()` or `write_table()` call opens and closes its own tunnel. No persistent connection pool.

---

## Handling Allocation Changes

The incremental mode (`--since`) only detects users with **new completions**. It does not detect changes to allocation structure (subjects/lessons added or removed). When allocation changes, stale rows remain in the DB for users who haven't completed anything since the change.

### When allocation changes — what to do

| Change type | Action |
|---|---|
| Subject / lessons added to a centre | `python main.py --centre-id <uuid> --output db` |
| Subject / lessons removed from a centre | `python main.py --centre-id <uuid> --output db` |
| Batch allocation changed | `python main.py --batch-id <uuid> --output db` |
| Trade allocation changed | `python main.py --trade-id <uuid> --output db` |
| Large structural change (many centres) | Full refresh: `python main.py --output db` |

A scoped refresh by `--centre-id` / `--batch-id` / `--trade-id` recalculates and replaces rows only for users in that scope. It is much faster than a full refresh and surgically correct.

> **Rule of thumb:** whenever the admin panel is used to add or remove subjects/lessons from a centre, batch, or trade — run the corresponding scoped refresh immediately afterward.

### Why incremental mode doesn't catch this

`--since` queries `learning_activities.completed_at` to find changed users. Allocation tables (`centre_subject`, `batch_subject`, `subject_trade`, `subject_ple_career_path`) have no corresponding event in the activity log — the pipeline has no way to know they changed unless told explicitly.

### Recommended schedule

```cron
# Daily incremental — picks up new completions (fast, ~minutes)
5 0 * * * cd /path/to/ael_v2_pipeline && venv/bin/python main.py \
  --since "$(date -d 'yesterday' '+%Y-%m-%d 00:00:00')" --log-file /var/log/ael.log

# Monthly full refresh — catches any allocation drift (slow, ~5h)
0 1 1 * * cd /path/to/ael_v2_pipeline && venv/bin/python main.py --log-file /var/log/ael_full.log
```

---

## Troubleshooting

**Only two DB tables appear after running**

Make sure you are not using `--output csv` alone — the DB tables are only written when `--output db` (default) or `--output both` is used. The all-lesson-types table (`main_learning_activity_myquest_ael_all_lesson_type`) is always written alongside the other two on every DB run.

**`--all-lesson-types` CSV files not appearing**

The flag only adds CSV files — the DB table is always written regardless. Make sure you also pass `--output csv` or `--output both`, otherwise no CSV files are written at all.

**Duplicate lesson rows**

Caused by a PLE user with more than one active career path. `fetch_allocation()` deduplicates automatically — the log line `deduplicated N duplicate (user_id, lesson_id) rows` confirms how many were dropped.

**Empty allocation for a user**

Common causes:
1. `student_details` row missing: `SELECT * FROM student_details WHERE user_id = '<uuid>'`
2. All subjects filtered by `year_to_map <= trade.duration`
3. For PLE: no active `ple_career_path_user` record (`status=1, deleted_at IS NULL`)
4. No `centre_subject` entries for the user's centre

**Staff user has no allocation**

Check:
1. `users.centre_id` is set for the staff user
2. The centre has entries in `centre_subject`
3. For Facilitators: at least one lesson in the centre has `facilitator_access = 1`
4. For Master Trainers: at least one lesson has `mastertrainer_access = 1`

**Empty completion (allocation is non-empty but output is empty)**

The user has no `completed = 1` records in `learning_activities` (or `facilitator_learning_activities`). This is expected for users who have been allocated but not yet started. Run `--dry-run` to confirm `total_allocated` is non-zero.

**Pipeline slow on large runs**

Use incremental mode (`--since`) for daily updates. Full refresh for 900K+ users takes ~5 hours. Reduce `ALLOC_CHUNK_SIZE` in `config.py` if you hit memory issues.

**`AuthenticationException` on SSH connect**

- Check `.pem` exists in `DB_Config/` with correct filename from `.env`
- Set permissions: `chmod 400 DB_Config/<keyfile>.pem`
- Verify SSH username matches the key

**`No route to host` / `Connection timed out`**

You need VPN or office network connectivity to reach the bastion hosts (`SOURCE_SSH_HOST`, `DEST_SSH_HOST` in `.env`).

**`ModuleNotFoundError`**

```bash
source venv/bin/activate
pip install -r requirements.txt
```

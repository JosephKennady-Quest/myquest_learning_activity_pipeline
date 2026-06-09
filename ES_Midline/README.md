# ES Midline — Mobile & Email Lookup Pipeline

A data pipeline that takes participant lists from Google Sheets (across multiple academic years), looks up each person in the QuestApp production database by **mobile number or email**, and enriches the output with app usage data.

---

## What it does

For each academic year sheet, the pipeline:

1. Fetches the participant list from a published Google Sheet (CSV format)
2. Extracts valid 10-digit mobile numbers and email addresses
3. Queries the QuestApp production database via SSH tunnel — matching users by **mobile OR email**
4. Deduplicates matched records using a priority rule (active > status=1 > type order 1→2→3→4)
5. Enriches each matched user with:
   - Batch statistics (total, completed, ongoing, deleted)
   - Last login date
   - Community feature usage (yes/no)
   - Number of batches created by the user
6. Left-joins back to the original sheet rows so every participant is in the output
7. Saves three separate output CSVs — one per academic year

---

## Output files

| File | Description |
|------|-------------|
| `lookup_final_results_2023-24.csv` | Final enriched output for 2023-24 cohort |
| `lookup_final_results_2024-25.csv` | Final enriched output for 2024-25 cohort |
| `lookup_final_results_2025-26.csv` | Final enriched output for 2025-26 cohort |
| `lookup_raw_results_<year>.csv` | Raw DB query results before deduplication |
| `lookup_cleaned_results_<year>.csv` | Deduplicated DB results before final join |

### Key output columns

| Column | Description |
|--------|-------------|
| *(all original sheet columns)* | Carried through as-is from Google Sheet |
| `id` | QuestApp user UUID |
| `mobile` | Mobile number from DB |
| `email` | Email from DB |
| `status` | User status in DB (1 = active) |
| `deleted_at` | Deletion timestamp if soft-deleted |
| `type` | User type (1=admin, 2=facilitator, 3=learner, 4=alumni) |
| `centre_id` / `centre_name` | Centre the user belongs to |
| `state` / `district` | Location |
| `number_of_batches` | Total batches at the user's centre |
| `completed_batch` | Completed batches |
| `ongoing` | Ongoing batches |
| `last_login` | Most recent login date |
| `feature_usage_in_community` | `yes` / `no` — whether user has community records |
| `batches_created_by_user` | Count of batches this user created |
| `InQuestApp` | `1` if matched in DB, `0` if not found |
| `user_valid_status` | Human-readable match status (see below) |
| `_match_source` | `mobile`, `email`, or blank — how the match was made |

### `user_valid_status` values

| Value | Meaning |
|-------|---------|
| `Valid facilitator record` | Active user, not deleted, type 1 or 2 |
| `User Registered as a Learner/Alumni` | Active but type 3 or 4 |
| `No valid record in QuestApp` | Deleted, inactive, or not found at all |

---

## Google Sheets source

The input data is a published Google Sheet with **3 tabs**, one per academic year.

**Base URL:**
```
https://docs.google.com/spreadsheets/d/e/2PACX-1vS_wdv3BisJ5oPdsN6VdQ8pfTSkxMYR14fcQOAwPcesCDnPUTQsl5ycolzd-1wckWFb12_YufSYdODt/pub?output=csv&gid={gid}
```

| Year | GID |
|------|-----|
| 2023-24 | `613295323` |
| 2024-25 | `0` |
| 2025-26 | `1498742244` |

### Column names per sheet

| Sheet | Mobile column | Email column | Notes |
|-------|--------------|--------------|-------|
| 2023-24 | `Mobile No` | `Email` | Single `Name` column, `12. Designation` |
| 2024-25 | `Mobile No` | `Email` | `First Name` + `Middle and Last Name`, `District` |
| 2025-26 | `Mobile No` | `Email` | Same as 2024-25, plus `Date of Birth` |

---

## Setup

### 1. Clone the repo

```bash
git clone <repo-url>
cd Priyanka_ES_Midline
```

### 2. Install dependencies

```bash
pip install pandas pymysql sshtunnel
```

### 3. Configure credentials

Copy the example config and fill in your values:

```bash
cp ES_Midline/config.example.py ES_Midline/DB_Config/config.py
```

Edit `DB_Config/config.py` with:
- Source bastion host IP and SSH username
- Path to your `.pem` SSH private key (place it in `DB_Config/`)
- RDS endpoint and DB credentials for the source (QuestApp production) database
- Destination DB credentials (only needed for `example_pipeline.py`)

> **Important:** `config.py` and all `.pem` files are in `.gitignore` and must never be committed.

### 4. Run the pipeline

```bash
python3 mobile_lookup_pipeline_2nd_request_with_three_year_data.py
```

The pipeline processes all three year sheets sequentially. Each sheet opens its own SSH tunnel, so only one tunnel is active at a time.

---

## Architecture

```
Google Sheets (3 tabs)
        │
        ▼
fetch_google_sheets_csv()        ← urllib + SSL bypass for macOS
        │
        ▼
extract_contacts()               ← extracts valid mobile numbers + emails
        │                           handles float-read numbers (e.g. 9876543210.0)
        ▼
query_users_by_mobile_or_email() ← SSH tunnel → QuestApp production DB
        │                           WHERE u.mobile IN (...) OR LOWER(u.email) IN (...)
        ▼
clean_and_deduplicate_users()    ← dedup by mobile first, then by email
        │                           priority: active > status=1 > type 1→2→3→4
        ▼
  ┌─────┼──────────────────────────────────┐
  │     │                                  │
  ▼     ▼                                  ▼
batch  login logs  community data  batches created
stats                               (all via SSH tunnel)
  │
  ▼
create_final_mapping()           ← dict-based lookup: mobile first, then email
        │                           left-join: every sheet row appears in output
        ▼
lookup_final_results_<year>.csv
```

---

## Deduplication logic

When multiple DB records match the same mobile/email:

1. **Remove soft-deleted records** (`deleted_at IS NOT NULL`) — keep active ones
2. If all are deleted → keep the one with the most recent `deleted_at`, preferring lower type number
3. **Keep only `status = 1`** (active) records
4. If none are status=1 → keep the first active record
5. **Prefer lower type number** (1 > 2 > 3 > 4)

---

## Files in this repo

```
Priyanka_ES_Midline/
├── mobile_lookup_pipeline_2nd_request_with_three_year_data.py   # main pipeline
├── example_pipeline.py                                           # generic SQL→analytics pipeline template
├── DB_Config/
│   ├── config.example.py    # template — copy to config.py and fill in credentials
│   └── (config.py)          # gitignored — contains real credentials
│   └── (*.pem)              # gitignored — SSH private keys
└── README.md
```

---

## Notes for future maintainers

- **New Google Sheet?** Update `SHEET_BASE_URL` and the `SHEETS` list (year + gid) at the top of the script. Get the GID from the sheet's publish URL: `File → Share → Publish to web → select sheet → CSV → copy link`.
- **Column name changed?** Update `MOBILE_COLUMN` and `EMAIL_COLUMN` constants at the top.
- **SSL error on macOS?** The `fetch_google_sheets_csv` function disables SSL verification — this is intentional for fetching public Google Sheets on macOS.
- **Mobile numbers read as floats?** The `normalize_mobile` helper inside `extract_contacts` handles this by converting via `int(float(x))`.
- **SSH tunnel timeouts?** Increase `read_timeout` in the relevant `pymysql.connect()` call. Current values: 1800s for the main user query, 900s for enrichment queries.

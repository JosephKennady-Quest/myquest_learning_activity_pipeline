# SQL AEL MyQuest Pipeline

This repository contains a MySQL-based AEL/MyQuest reporting pipeline. The current runner executes the one-record-per-user production SQL once for each centre ID and appends all centre outputs into one destination table.

The pipeline is designed for cases where the centre list is selected separately, for example:

```sql
SELECT c.id
FROM centres c
LIMIT 10;
```

The centre IDs are read by Python, injected one by one into the `params` CTE of the main SQL, and written into the same target table.

## Repository Layout

```text
.
├── config.py
├── db.py
├── run_production_users_by_centre.py
├── sql_queries/
│   ├── production_user_one_record_subject_project_combo.sql
│   └── centre_ids_limit_10.sql
├── .env.example
└── .gitignore
```

Key files:

- `run_production_users_by_centre.py`: Python entry point for centre-by-centre execution.
- `sql_queries/production_user_one_record_subject_project_combo.sql`: Main MySQL 8 SQL query that returns one row per user.
- `sql_queries/centre_ids_limit_10.sql`: Safe example centre-list query.
- `db.py`: MySQL connection, SSH tunnel, fetch, and write helpers.
- `config.py`: Environment-driven source and destination DB configuration.
- `.env.example`: Placeholder environment configuration. Copy this to `.env` locally and fill in real values.

## What The Runner Does

For each centre ID:

1. Reads the main SQL template.
2. Replaces values inside the `params` CTE:
   - `user_id = NULL`
   - `centre_id = current centre ID`
   - `batch_id = NULL`
3. Runs the SQL against the source database.
4. Appends the returned rows into one destination table.

The target table is not split by centre. All rows are written into the same table, with `centre_id` available as a column in the output.

## Requirements

- Python 3.10 or newer
- MySQL 8 or newer on the source database
- Python packages:

```bash
pip install -r requirements.txt
```

The source SQL uses CTEs and window functions such as `ROW_NUMBER()`, so MySQL 8+ is required.

## Environment Setup

Copy the example file:

```bash
cp .env.example .env
```

Fill in `.env` with your local database and SSH tunnel values.

Required source DB variables:

```text
SOURCE_SSH_HOST=
SOURCE_SSH_PORT=
SOURCE_SSH_USER=
SOURCE_SSH_PKEY_FILE=
SOURCE_RDS_HOST=
SOURCE_RDS_PORT=
SOURCE_DB_USER=
SOURCE_DB_PASSWORD=
SOURCE_DB_NAME=
```

Required destination DB variables:

```text
DEST_SSH_HOST=
DEST_SSH_PORT=
DEST_SSH_USER=
DEST_SSH_PKEY_FILE=
DEST_RDS_HOST=
DEST_RDS_PORT=
DEST_DB_USER=
DEST_DB_PASSWORD=
DEST_DB_NAME=
```

Private key files should be stored in `DB_Config/`.

Do not commit `.env`, private keys, or `DB_Config/`.

## Centre List SQL

Use a separate SQL file to control which centres are processed.

A safe example is included:

```bash
sql_queries/centre_ids_limit_10.sql
```

Example content:

```sql
SELECT c.id
FROM centres c
LIMIT 10;
```

For local work, you can create your own:

```bash
sql_queries/centre_ids.sql
```

That file is ignored by Git because it may contain real programme IDs, project IDs, centre filters, or other operational details.

## Run The Pipeline

Run with the example centre list and recreate the destination table on the first non-empty result:

```bash
python3 run_production_users_by_centre.py \
  --centre-sql-path sql_queries/centre_ids_limit_10.sql \
  --target-table production_users_one_record \
  --replace-target
```

Run with your local centre-list SQL:

```bash
python3 run_production_users_by_centre.py \
  --centre-sql-path sql_queries/centre_ids.sql \
  --target-table production_users_one_record \
  --replace-target
```

Append to the existing destination table without recreating it:

```bash
python3 run_production_users_by_centre.py \
  --centre-sql-path sql_queries/centre_ids.sql \
  --target-table production_users_one_record
```

Run the default first 10 centres without a separate centre-list file:

```bash
python3 run_production_users_by_centre.py \
  --target-table production_users_one_record \
  --replace-target
```

Run all centres:

```bash
python3 run_production_users_by_centre.py \
  --limit 0 \
  --target-table production_users_one_record \
  --replace-target
```

## Command Options

```text
--sql-path
    Path to the main SQL file.
    Default: sql_queries/production_user_one_record_subject_project_combo.sql

--centre-sql-path
    Optional SQL file that returns centre IDs in the first column.

--target-table
    Destination table name.
    Default: production_users_one_record

--limit
    Number of centres to process when --centre-sql-path is not provided.
    Default: 10
    Use 0 to process all centres.

--replace-target
    Recreate the target table on the first non-empty centre result.
    Without this flag, results are appended to the existing table.
```

## Rerun Behaviour

Use `--replace-target` when you want a fresh rebuild:

```bash
python3 run_production_users_by_centre.py \
  --centre-sql-path sql_queries/centre_ids.sql \
  --target-table production_users_one_record \
  --replace-target
```

Without `--replace-target`, the script appends rows. This is useful when extending a table, but it can create duplicates if you rerun the same centre list.

## Sensitive Information Policy

The repository is configured to avoid committing local secrets:

- `.env` is ignored.
- `*.pem` is ignored.
- `DB_Config/` is ignored.
- `sql_queries/centre_ids.sql` is ignored because local centre-list queries may include real IDs.
- Generated outputs under `output/` are ignored.

Before pushing to GitHub, check:

```bash
git status --short
git diff --cached
```

Do not stage files containing:

- real passwords
- private keys
- database hostnames
- SSH usernames
- production-only IDs that should not be public
- exported data

## Validation

Syntax-check the Python files:

```bash
python3 -m py_compile db.py run_production_users_by_centre.py
```

The pipeline itself requires live database access, so a full end-to-end run should only be done from an environment with valid SSH and DB credentials.

## Troubleshooting

### `TypeError: not enough arguments for format string`

This can happen when PyMySQL receives SQL containing literal `%` characters, such as:

```sql
LIKE '%ASSESSMENT%'
```

The current `db.fetch()` implementation avoids that by calling `execute(sql)` when no query parameters are provided.

### Empty result for a centre

The runner skips writes for centres that return no rows.

Check:

- the centre exists and is active
- users exist for that centre
- the SQL filters match the required user types
- related project, batch, subject, and lesson mappings exist

### Duplicate rows

If you rerun without `--replace-target`, rows are appended. Use `--replace-target` for a clean rebuild.

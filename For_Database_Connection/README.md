# Database Connection via SSH Tunnel — `db_sql`

Connect to AWS RDS MySQL instances through SSH bastion hosts using Python.  
Works on **macOS**, **Linux**, and **Windows** (Python 3.8+).

---

## How it works

```
Your Script
    │
    ├─ db_qapp_production()  ──►  SSH Bastion (52.66.225.6)  ──►  RDS: quest_rearch_production
    │
    └─ db_analytics()        ──►  SSH Bastion (15.206.29.129) ──►  RDS: analytics_
```

Each function:
1. Reads credentials from `.env`
2. Opens an SSH tunnel through the bastion server using the PEM key
3. Connects to MySQL through that tunnel
4. Automatically closes the tunnel and connection when the `with` block exits

---

## Directory Structure

```
For_Database_Connection/
├── DB_Config/                   # Create this folder locally — gitignored
│   ├── joseph_prod.pem          # Bastion key for quest_rearch_production
│   └── analytics_master.pem    # Bastion key for analytics_
├── .env                         # Created from .env.example — gitignored, never commit
├── .env.example                 # Template showing all required variables — committed
├── .gitignore
├── db_sql.py                    # The connection library — import this in your pipelines
├── requirements.txt
└── README.md
```

---

## One-time Setup

### 1. Create and activate a virtual environment

**macOS / Linux**
```bash
python3 -m venv .venv
source .venv/bin/activate
```

**Windows (Command Prompt)**
```cmd
python -m venv .venv
.venv\Scripts\activate.bat
```

**Windows (PowerShell)**
```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
# If blocked, run once as Administrator: Set-ExecutionPolicy -Scope CurrentUser RemoteSigned
```

### 2. Install dependencies

```bash
pip install -r requirements.txt
```

### 3. Set up credentials

Copy the example file and fill in the real values:

```bash
cp .env.example .env
```

Then open `.env` and replace every `<placeholder>` with the actual value.  
Get the credentials and PEM files from your team lead or AWS console.

### 4. Place PEM files

Create the `DB_Config/` folder and drop both `.pem` keys inside it:

```bash
mkdir DB_Config
# copy joseph_prod.pem and analytics_master.pem into DB_Config/
```

On macOS/Linux the script automatically fixes PEM file permissions to `0400`. On Windows no chmod is needed.

### 5. Verify the setup

```bash
python3 db_sql.py
```

You should see both databases connect and print their current timestamp.

---

## Using `db_sql` in your pipeline scripts

### Import

```python
from db_sql import db_analytics, db_qapp_production
```

> Your pipeline script must either live inside `For_Database_Connection/` or you must add it to `sys.path` — see the path setup section below.

---

### Pattern 1 — Fetch rows from the source DB

```python
from db_sql import db_qapp_production

with db_qapp_production() as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT id, name, email FROM users WHERE active = 1")
        users = cur.fetchall()

for user in users:
    print(user["id"], user["name"])
```

---

### Pattern 2 — Write rows to the analytics DB

```python
from db_sql import db_analytics

rows_to_insert = [
    {"student_id": 101, "score": 88.5},
    {"student_id": 102, "score": 91.0},
]

with db_analytics() as conn:
    with conn.cursor() as cur:
        cur.executemany(
            "INSERT INTO student_scores (student_id, score) VALUES (%(student_id)s, %(score)s)",
            rows_to_insert,
        )
    conn.commit()
```

> Always call `conn.commit()` after INSERT / UPDATE / DELETE. SELECT queries do not need it.

---

### Pattern 3 — Read from source, transform, write to destination (ETL)

```python
from db_sql import db_qapp_production, db_analytics

# Step 1: Extract
with db_qapp_production() as conn:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT
                user_id,
                COUNT(*) AS total_sessions,
                MAX(created_at) AS last_active
            FROM sessions
            WHERE created_at >= '2026-01-01'
            GROUP BY user_id
        """)
        records = cur.fetchall()

# Step 2: Transform
transformed = [
    {
        "user_id": r["user_id"],
        "total_sessions": r["total_sessions"],
        "last_active": r["last_active"],
    }
    for r in records
    if r["total_sessions"] > 0
]

# Step 3: Load
with db_analytics() as conn:
    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO user_activity_summary (user_id, total_sessions, last_active)
            VALUES (%(user_id)s, %(total_sessions)s, %(last_active)s)
            ON DUPLICATE KEY UPDATE
                total_sessions = VALUES(total_sessions),
                last_active    = VALUES(last_active)
            """,
            transformed,
        )
    conn.commit()

print(f"Loaded {len(transformed)} records.")
```

---

### Pattern 4 — Chunked processing for large tables

```python
from db_sql import db_qapp_production

CHUNK_SIZE = 1000
offset = 0

with db_qapp_production() as conn:
    while True:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM events LIMIT %s OFFSET %s",
                (CHUNK_SIZE, offset),
            )
            chunk = cur.fetchall()

        if not chunk:
            break

        # process chunk here
        print(f"Processing rows {offset} – {offset + len(chunk)}")
        offset += len(chunk)
```

---

### Pattern 5 — Both DBs open at the same time

```python
from db_sql import db_qapp_production, db_analytics

with db_qapp_production() as src, db_analytics() as dst:
    with src.cursor() as src_cur, dst.cursor() as dst_cur:
        src_cur.execute("SELECT id, value FROM source_table LIMIT 500")
        rows = src_cur.fetchall()

        dst_cur.executemany(
            "INSERT IGNORE INTO dest_table (id, value) VALUES (%(id)s, %(value)s)",
            rows,
        )
    dst.commit()
```

---

## Calling from a script outside this folder

If your pipeline lives in a different directory, add the path to `db_sql` at the top:

```python
import sys
sys.path.insert(0, "/absolute/path/to/For_Database_Connection")

from db_sql import db_analytics, db_qapp_production
```

Or use a relative path:

```python
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "For_Database_Connection"))

from db_sql import db_analytics, db_qapp_production
```

---

## .env reference

```ini
# ── Source DB (quest_rearch_production) ──────────────────────────────────────
SOURCE_SSH_HOST=<bastion IP>
SOURCE_SSH_PORT=22
SOURCE_SSH_USER=<ssh user>
SOURCE_SSH_PKEY_FILE=joseph_prod.pem      # filename only — must be in DB_Config/

SOURCE_RDS_HOST=<rds endpoint>
SOURCE_RDS_PORT=3306
SOURCE_DB_USER=<db user>
SOURCE_DB_PASSWORD=<password>             # wrap in single quotes if it contains # or spaces
SOURCE_DB_NAME=quest_rearch_production

# ── Destination DB (analytics_) ──────────────────────────────────────────────
DEST_SSH_HOST=<bastion IP>
DEST_SSH_PORT=22
DEST_SSH_USER=<ssh user>
DEST_SSH_PKEY_FILE=analytics_master.pem  # filename only — must be in DB_Config/

DEST_RDS_HOST=<rds endpoint>
DEST_RDS_PORT=3306
DEST_DB_USER=<db user>
DEST_DB_PASSWORD='<password>'            # single quotes required if password contains #
DEST_DB_NAME=analytics_
```

> Passwords containing `#` must be wrapped in single quotes in the `.env` file.  
> `python-dotenv` strips the quotes automatically, so the password is passed to MySQL as-is.

---

## Troubleshooting

| Error | Cause | Fix |
|---|---|---|
| `FileNotFoundError: PEM file not found` | PEM filename in `.env` doesn't match the file in `DB_Config/` | Check spelling and case |
| `Unable to load private key` | PEM file is empty or corrupted | Verify with `openssl pkey -in DB_Config/your.pem -check -noout` |
| `Authentication (publickey) failed` | Wrong PEM key for that bastion user | Get the correct `.pem` from AWS EC2 → Key Pairs or your team |
| `OperationalError: Can't connect to MySQL` | Wrong `RDS_HOST` or the bastion can't reach the RDS endpoint | Double-check the RDS hostname in `.env` |
| `ModuleNotFoundError: No module named 'db_sql'` | Script is outside this folder and path isn't set | Add `sys.path.insert` as shown above |
| PowerShell execution policy error | Scripts blocked on Windows | Run `Set-ExecutionPolicy -Scope CurrentUser RemoteSigned` once as Administrator |

---

## Security Notes

- **Never commit `.env` or `.pem` files.** Add to `.gitignore`:
  ```
  .env
  DB_Config/
  ```
- Rotate credentials immediately if they are accidentally pushed to git.
- PEM files must have permissions `0400` on macOS/Linux — the script handles this automatically.

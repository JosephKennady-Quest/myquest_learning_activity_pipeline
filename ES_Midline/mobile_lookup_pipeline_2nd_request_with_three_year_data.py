# Mobile Number / Email Lookup Pipeline
# Reads 3 Google Sheets (one per year), matches by mobile OR email, outputs 3 CSVs

import pandas as pd
import pymysql
from sshtunnel import SSHTunnelForwarder
from datetime import datetime
import ssl
import urllib.request
from pathlib import Path
import DB_Config.config as config


# =========================
# CONFIG
# =========================
SHEET_BASE_URL = (
    "https://docs.google.com/spreadsheets/d/e/"
    "2PACX-1vS_wdv3BisJ5oPdsN6VdQ8pfTSkxMYR14fcQOAwPcesCDnPUTQsl5ycolzd-1wckWFb12_YufSYdODt"
    "/pub?output=csv&gid={gid}"
)

# GIDs for each year sheet
SHEETS = [
    {"year": "2023-24", "gid": "613295323"},
    {"year": "2024-25", "gid": "0"},
    {"year": "2025-26", "gid": "1498742244"},
]

MOBILE_COLUMN = "Mobile No"
EMAIL_COLUMN = "Email"
BASE_DIR = Path(__file__).resolve().parent


def get_absolute_key_path(relative_path):
    abs_path = BASE_DIR / relative_path
    if not abs_path.exists():
        raise FileNotFoundError(f"SSH key not found: {abs_path}")
    return str(abs_path)


# =========================
# HELPER FUNCTIONS
# =========================
def fetch_google_sheets_csv(url):
    """Fetch CSV from Google Sheets public URL."""
    print(f"📥 Fetching CSV from Google Sheets...")
    try:
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE

        with urllib.request.urlopen(url, context=ssl_context) as response:
            df = pd.read_csv(response)

        print(f"✅ Successfully read CSV ({len(df)} rows)")
        return df
    except Exception as e:
        print(f"❌ Error fetching CSV: {e}")
        return None


def extract_contacts(df):
    """Extract valid mobile numbers and emails from the sheet dataframe."""
    print(f"\n🔍 Extracting contacts (mobile + email)...")

    mobile_numbers = []
    emails = []

    if MOBILE_COLUMN in df.columns:
        # Convert float-read numbers (e.g. 9876543210.0) to clean strings before matching
        def normalize_mobile(x):
            if pd.isna(x) or str(x).strip() in ('', 'nan'):
                return ''
            try:
                return str(int(float(str(x).strip())))
            except (ValueError, TypeError):
                return str(x).strip()

        mobile_series = df[MOBILE_COLUMN].apply(normalize_mobile)
        valid_mobiles = mobile_series[mobile_series.str.match(r'^\d{10}$')]
        mobile_numbers = valid_mobiles.unique().tolist()
        print(f"   📱 Found {len(mobile_numbers)} unique 10-digit mobile numbers")
    else:
        print(f"   ⚠️ Column '{MOBILE_COLUMN}' not found")

    if EMAIL_COLUMN in df.columns:
        email_series = df[EMAIL_COLUMN].astype(str).str.strip().str.lower()
        valid_emails = email_series[email_series.str.contains(r'^[^@\s]+@[^@\s]+\.[^@\s]+$', regex=True)]
        emails = valid_emails.unique().tolist()
        print(f"   📧 Found {len(emails)} unique email addresses")
    else:
        print(f"   ⚠️ Column '{EMAIL_COLUMN}' not found")

    return mobile_numbers, emails


def query_users_by_mobile_or_email(mobile_numbers, emails):
    """Query source database for users matching mobile numbers OR emails."""

    if not mobile_numbers and not emails:
        print("❌ No contacts to search")
        return None

    print(f"\n📡 Querying source database...")
    print(f"   Mobile numbers: {len(mobile_numbers)}, Emails: {len(emails)}")
    print(f"🕒 Started at: {datetime.now()}")

    source_cfg = config.CONFIG["source"]
    ssh_key_path = get_absolute_key_path(source_cfg["ssh"]["pkey_path"])

    conditions = []
    if mobile_numbers:
        mobile_placeholders = ",".join(f"'{m}'" for m in mobile_numbers)
        conditions.append(f"u.mobile IN ({mobile_placeholders})")
    if emails:
        email_placeholders = ",".join(f"'{e}'" for e in emails)
        conditions.append(f"LOWER(u.email) IN ({email_placeholders})")

    where_clause = " OR ".join(conditions)

    query = f"""
    SELECT
        u.id,
        u.mobile,
        LOWER(u.email) AS email,
        u.status,
        u.deleted_at,
        u.`type`,
        c.id AS centre_id,
        c.name AS centre_name,
        s.name AS state,
        d.name AS district
    FROM
        quest_rearch_production.users u
    LEFT JOIN quest_rearch_production.centres c ON c.id = u.centre_id
    LEFT JOIN quest_rearch_production.states s ON s.id = c.state_id
    LEFT JOIN quest_rearch_production.districts d ON d.id = c.district_id
    WHERE
        {where_clause}
    ORDER BY u.id
    """

    try:
        with SSHTunnelForwarder(
            (source_cfg["ssh"]["host"], source_cfg["ssh"].get("port", 22)),
            ssh_username=source_cfg["ssh"]["username"],
            ssh_pkey=ssh_key_path,
            remote_bind_address=(source_cfg["ssh"]["remote_bind_address"], source_cfg["ssh"]["remote_bind_port"]),
            set_keepalive=30
        ) as tunnel:

            conn = pymysql.connect(
                host="127.0.0.1",
                port=tunnel.local_bind_port,
                user=source_cfg["db"]["user"],
                password=source_cfg["db"]["password"],
                database=source_cfg["db"]["database"],
                cursorclass=pymysql.cursors.DictCursor,
                connect_timeout=60,
                read_timeout=1800,
                autocommit=True
            )

            cursor = conn.cursor()
            cursor.execute(query)
            results = cursor.fetchall()

            cursor.close()
            conn.close()

            print(f"✅ Query completed ({len(results)} users found)")
            print(f"🕓 Ended at: {datetime.now()}")
            return results

    except Exception as e:
        print(f"❌ Database query error: {e}")
        return None


def query_batch_statistics(user_ids):
    """Query batch statistics for users."""
    if not user_ids:
        return None

    print(f"\n📦 Fetching batch statistics for {len(user_ids)} users...")
    source_cfg = config.CONFIG["source"]
    ssh_key_path = get_absolute_key_path(source_cfg["ssh"]["pkey_path"])
    placeholders = ",".join(f"'{u}'" for u in user_ids)

    query = f"""
    SELECT
        c.id AS centre_id,
        COUNT(DISTINCT b.id) AS number_of_batches,
        COUNT(DISTINCT CASE WHEN b.status = 2 THEN b.id END) AS completed_batch,
        COUNT(DISTINCT CASE WHEN b.status = 1 THEN b.id END) AS ongoing,
        COUNT(DISTINCT CASE WHEN b.status = 4 THEN b.id END) AS deleted_inactive
    FROM quest_rearch_production.users u
    LEFT JOIN quest_rearch_production.centres c ON c.id = u.centre_id
    LEFT JOIN quest_rearch_production.batches b ON b.centre_id = c.id AND b.deleted_at IS NULL
    WHERE u.id IN ({placeholders})
    GROUP BY c.id
    """

    try:
        with SSHTunnelForwarder(
            (source_cfg["ssh"]["host"], source_cfg["ssh"].get("port", 22)),
            ssh_username=source_cfg["ssh"]["username"],
            ssh_pkey=ssh_key_path,
            remote_bind_address=(source_cfg["ssh"]["remote_bind_address"], source_cfg["ssh"]["remote_bind_port"]),
            set_keepalive=30
        ) as tunnel:

            conn = pymysql.connect(
                host="127.0.0.1",
                port=tunnel.local_bind_port,
                user=source_cfg["db"]["user"],
                password=source_cfg["db"]["password"],
                database=source_cfg["db"]["database"],
                cursorclass=pymysql.cursors.DictCursor,
                connect_timeout=60,
                read_timeout=900,
                autocommit=True
            )

            cursor = conn.cursor()
            cursor.execute(query)
            results = cursor.fetchall()
            cursor.close()
            conn.close()

            print(f"✅ Batch statistics fetched ({len(results)} centres)")
            return results

    except Exception as e:
        print(f"❌ Error fetching batch statistics: {e}")
        return None


def query_login_logs(user_ids):
    """Query last login date for users."""
    if not user_ids:
        return None

    print(f"\n🔐 Fetching login logs for {len(user_ids)} users...")
    source_cfg = config.CONFIG["source"]
    ssh_key_path = get_absolute_key_path(source_cfg["ssh"]["pkey_path"])
    placeholders = ",".join(f"'{u}'" for u in user_ids)

    query = f"""
    SELECT
        user_id,
        MAX(created_at) AS last_login
    FROM quest_rearch_production.login_logs
    WHERE user_id IN ({placeholders})
    GROUP BY user_id
    """

    try:
        with SSHTunnelForwarder(
            (source_cfg["ssh"]["host"], source_cfg["ssh"].get("port", 22)),
            ssh_username=source_cfg["ssh"]["username"],
            ssh_pkey=ssh_key_path,
            remote_bind_address=(source_cfg["ssh"]["remote_bind_address"], source_cfg["ssh"]["remote_bind_port"]),
            set_keepalive=30
        ) as tunnel:

            conn = pymysql.connect(
                host="127.0.0.1",
                port=tunnel.local_bind_port,
                user=source_cfg["db"]["user"],
                password=source_cfg["db"]["password"],
                database=source_cfg["db"]["database"],
                cursorclass=pymysql.cursors.DictCursor,
                connect_timeout=60,
                read_timeout=900,
                autocommit=True
            )

            cursor = conn.cursor()
            cursor.execute(query)
            results = cursor.fetchall()
            cursor.close()
            conn.close()

            print(f"✅ Login logs fetched ({len(results)} users with login data)")
            return results

    except Exception as e:
        print(f"❌ Error fetching login logs: {e}")
        return None


def query_communities(user_ids):
    """Query communities table to check feature usage for users."""
    if not user_ids:
        return None

    print(f"\n🏘️ Fetching community data for {len(user_ids)} users...")
    source_cfg = config.CONFIG["source"]
    ssh_key_path = get_absolute_key_path(source_cfg["ssh"]["pkey_path"])
    placeholders = ",".join(f"'{u}'" for u in user_ids)

    query = f"""
    SELECT
        c.user_id,
        COUNT(c.id) AS community_count
    FROM quest_rearch_production.communities AS c
    WHERE c.user_id IN ({placeholders})
    GROUP BY c.user_id
    """

    try:
        with SSHTunnelForwarder(
            (source_cfg["ssh"]["host"], source_cfg["ssh"].get("port", 22)),
            ssh_username=source_cfg["ssh"]["username"],
            ssh_pkey=ssh_key_path,
            remote_bind_address=(source_cfg["ssh"]["remote_bind_address"], source_cfg["ssh"]["remote_bind_port"]),
            set_keepalive=30
        ) as tunnel:

            conn = pymysql.connect(
                host="127.0.0.1",
                port=tunnel.local_bind_port,
                user=source_cfg["db"]["user"],
                password=source_cfg["db"]["password"],
                database=source_cfg["db"]["database"],
                cursorclass=pymysql.cursors.DictCursor,
                connect_timeout=60,
                read_timeout=900,
                autocommit=True
            )

            cursor = conn.cursor()
            cursor.execute(query)
            results = cursor.fetchall()
            cursor.close()
            conn.close()

            print(f"✅ Community data fetched ({len(results)} users with community records)")
            return results

    except Exception as e:
        print(f"❌ Error fetching community data: {e}")
        return None


def query_batches_created_by_user(user_ids):
    """Query batches table to count batches created by each user."""
    if not user_ids:
        return None

    print(f"\n📦 Fetching batches created by {len(user_ids)} users...")
    source_cfg = config.CONFIG["source"]
    ssh_key_path = get_absolute_key_path(source_cfg["ssh"]["pkey_path"])
    placeholders = ",".join(f"'{u}'" for u in user_ids)

    query = f"""
    SELECT
        b.created_by AS user_id,
        COUNT(DISTINCT b.id) AS batches_created_by_user
    FROM quest_rearch_production.batches AS b
    WHERE b.created_by IN ({placeholders})
    GROUP BY b.created_by
    """

    try:
        with SSHTunnelForwarder(
            (source_cfg["ssh"]["host"], source_cfg["ssh"].get("port", 22)),
            ssh_username=source_cfg["ssh"]["username"],
            ssh_pkey=ssh_key_path,
            remote_bind_address=(source_cfg["ssh"]["remote_bind_address"], source_cfg["ssh"]["remote_bind_port"]),
            set_keepalive=30
        ) as tunnel:

            conn = pymysql.connect(
                host="127.0.0.1",
                port=tunnel.local_bind_port,
                user=source_cfg["db"]["user"],
                password=source_cfg["db"]["password"],
                database=source_cfg["db"]["database"],
                cursorclass=pymysql.cursors.DictCursor,
                connect_timeout=60,
                read_timeout=900,
                autocommit=True
            )

            cursor = conn.cursor()
            cursor.execute(query)
            results = cursor.fetchall()
            cursor.close()
            conn.close()

            print(f"✅ Batches created data fetched ({len(results)} users with created batches)")
            return results

    except Exception as e:
        print(f"❌ Error fetching batches created data: {e}")
        return None


def clean_and_deduplicate_users(df_results, year_label):
    """Clean and deduplicate users with specific filtering rules."""
    print(f"\n🧹 Starting deduplication process...")
    print(f"📊 Initial records: {len(df_results)}")

    raw_csv_path = BASE_DIR / f"lookup_raw_results_{year_label}.csv"
    df_results.to_csv(raw_csv_path, index=False)
    print(f"💾 Raw results saved to: {raw_csv_path}")

    df_results['deleted_at'] = pd.to_datetime(df_results['deleted_at'], errors='coerce')
    type_priority = {1: 1, 2: 2, 3: 3, 4: 4}

    # Deduplicate by mobile first, then by email for remaining duplicates
    grouped = df_results.groupby('mobile', dropna=True)
    cleaned_records = []
    processed_ids = set()

    for _, group in grouped:
        record = _pick_best_record(group, type_priority)
        cleaned_records.append(record)
        processed_ids.add(record['id'])

    # Handle rows that only matched by email (no valid mobile) — not already processed
    email_only = df_results[~df_results['id'].isin(processed_ids)]
    if not email_only.empty:
        for _, group in email_only.groupby('email', dropna=True):
            record = _pick_best_record(group, type_priority)
            if record['id'] not in processed_ids:
                cleaned_records.append(record)
                processed_ids.add(record['id'])

    df_cleaned = pd.DataFrame(cleaned_records)

    if 'type_rank' in df_cleaned.columns:
        df_cleaned = df_cleaned.drop('type_rank', axis=1)

    cleaned_csv_path = BASE_DIR / f"lookup_cleaned_results_{year_label}.csv"
    df_cleaned.to_csv(cleaned_csv_path, index=False)

    print(f"✅ Deduplication completed!")
    print(f"📊 Final unique records: {len(df_cleaned)}")
    print(f"💾 Cleaned results saved to: {cleaned_csv_path}")

    return df_cleaned


def _pick_best_record(group, type_priority):
    """Pick the single best record from a group of duplicates."""
    records = group.copy()

    if len(records) == 1:
        return records.iloc[0]

    active_records = records[records['deleted_at'].isna()]

    if len(active_records) == 0:
        records_copy = records.copy()
        records_copy['type_rank'] = records_copy['type'].map(type_priority).fillna(999)
        records_copy = records_copy.sort_values(['type_rank', 'deleted_at'], ascending=[True, False])
        return records_copy.iloc[0]

    if len(active_records) == 1:
        return active_records.iloc[0]

    status_active = active_records[active_records['status'] == 1]

    if len(status_active) == 0:
        return active_records.iloc[0]

    if len(status_active) == 1:
        return status_active.iloc[0]

    status_active = status_active.copy()
    status_active['type_rank'] = status_active['type'].map(type_priority).fillna(999)
    status_active = status_active.sort_values('type_rank', ascending=True)
    return status_active.iloc[0]


def create_final_mapping(df_sheet, df_cleaned, year_label):
    """
    Left-join sheet rows to cleaned DB records.
    Priority: match by mobile first, then by email for unmatched rows.
    """
    print(f"\n🔗 Creating final mapping for {year_label}...")

    df_sheet = df_sheet.copy().reset_index(drop=True)
    df_db = df_cleaned.reset_index(drop=True).copy()

    # Build lookup dicts: mobile → db row, email → db row
    mobile_lookup = {}
    for _, row in df_db.iterrows():
        m = str(row.get('mobile', '')).strip()
        if m and m != 'nan':
            mobile_lookup[m] = row

    email_lookup = {}
    for _, row in df_db.iterrows():
        e = str(row.get('email', '')).strip().lower()
        if e and e != 'nan':
            email_lookup[e] = row

    db_cols = list(df_db.columns)

    # For each sheet row, find the best DB match
    matched_rows = []
    match_sources = []

    for _, sheet_row in df_sheet.iterrows():
        raw_mobile = sheet_row.get(MOBILE_COLUMN, '')
        try:
            mobile_key = str(int(float(raw_mobile))) if pd.notna(raw_mobile) and str(raw_mobile).strip() not in ('', 'nan') else ''
        except (ValueError, TypeError):
            mobile_key = str(raw_mobile).strip()
        email_key = str(sheet_row.get(EMAIL_COLUMN, '')).strip().lower() if EMAIL_COLUMN in df_sheet.columns else ''

        db_row = None
        source = None

        if mobile_key and mobile_key != 'nan' and mobile_key in mobile_lookup:
            db_row = mobile_lookup[mobile_key]
            source = 'mobile'
        elif email_key and email_key != 'nan' and email_key in email_lookup:
            db_row = email_lookup[email_key]
            source = 'email'

        if db_row is not None:
            matched_rows.append(db_row.to_dict())
        else:
            matched_rows.append({c: None for c in db_cols})
        match_sources.append(source)

    df_db_matched = pd.DataFrame(matched_rows)

    # Combine sheet columns with matched DB columns (prefix DB cols to avoid name clashes, then rename)
    sheet_cols = list(df_sheet.columns)
    overlap = [c for c in db_cols if c in sheet_cols]
    rename_db = {c: f'db_{c}' for c in overlap}
    df_db_matched = df_db_matched.rename(columns=rename_db)

    df_final = pd.concat([df_sheet.reset_index(drop=True), df_db_matched.reset_index(drop=True)], axis=1)
    df_final['_match_source'] = match_sources

    # Restore original names for overlapping DB columns (drop sheet version, rename db_ version)
    for orig in overlap:
        db_col = f'db_{orig}'
        if db_col in df_final.columns:
            df_final = df_final.drop(columns=[orig])
            df_final = df_final.rename(columns={db_col: orig})

    # Ensure all expected DB columns are present
    for c in db_cols:
        if c not in df_final.columns:
            df_final[c] = None

    # InQuestApp flag and status
    df_final['InQuestApp'] = (df_final['id'].notna()).astype(int)

    def compute_user_valid_status(row):
        if pd.isna(row['id']):
            return 'No valid record in QuestApp'
        if pd.notna(row['deleted_at']) or row['status'] != 1:
            return 'No valid record in QuestApp'
        if row['type'] in [3, 4]:
            return 'User Registered as a Learner/Alumni'
        if row['type'] in [1, 2]:
            return 'Valid facilitator record'
        return 'No valid record in QuestApp'

    df_final['user_valid_status'] = df_final.apply(compute_user_valid_status, axis=1)

    # Reorder: keep sheet columns first, then DB columns, then flags
    trailing = ['InQuestApp', 'user_valid_status', '_match_source', 'batches_created_by_user', 'feature_usage_in_community']
    front_cols = [c for c in df_final.columns if c not in trailing]
    df_final = df_final[[c for c in front_cols if c in df_final.columns] + [c for c in trailing if c in df_final.columns]]

    final_csv_path = BASE_DIR / f"lookup_final_results_{year_label}.csv"
    df_final.to_csv(final_csv_path, index=False)

    in_app = (df_final['InQuestApp'] == 1).sum()
    not_in_app = (df_final['InQuestApp'] == 0).sum()

    print(f"✅ Mapping completed for {year_label}!")
    print(f"📊 Total rows: {len(df_final)}  |  In Quest App: {in_app}  |  Not found: {not_in_app}")
    print(f"💾 Final results saved to: {final_csv_path}")

    return df_final


def process_sheet(sheet_cfg):
    """Run the full pipeline for a single year sheet."""
    year = sheet_cfg["year"]
    gid = sheet_cfg["gid"]
    url = SHEET_BASE_URL.format(gid=gid)

    print(f"\n{'='*60}")
    print(f"  Processing sheet: {year}  (gid={gid})")
    print(f"{'='*60}")

    # Step 1: Fetch sheet
    df_sheet = fetch_google_sheets_csv(url)
    if df_sheet is None:
        print(f"⚠️ Skipping {year} — could not fetch sheet")
        return

    # Step 2: Extract contacts
    mobile_numbers, emails = extract_contacts(df_sheet)
    if not mobile_numbers and not emails:
        print(f"⚠️ No contacts found for {year}")
        return

    # Step 3: Query DB
    query_results = query_users_by_mobile_or_email(mobile_numbers, emails)
    if not query_results:
        print(f"⚠️ No DB results for {year}")
        return

    df_raw = pd.DataFrame(query_results)
    print(f"\n📋 Raw DB results (head 5):")
    print(df_raw.head(5).to_string())

    # Step 4: Clean & deduplicate
    df_cleaned = clean_and_deduplicate_users(df_raw, year)

    user_ids = df_cleaned['id'].unique().tolist()

    # Step 5: Batch statistics
    batch_stats = query_batch_statistics(user_ids)
    if batch_stats:
        df_batch_stats = pd.DataFrame(batch_stats)
        df_cleaned = df_cleaned.merge(df_batch_stats, on='centre_id', how='left')

    # Step 6: Login logs
    login_stats = query_login_logs(user_ids)
    if login_stats:
        df_login_stats = pd.DataFrame(login_stats).rename(columns={'user_id': 'id'})
        df_cleaned = df_cleaned.merge(df_login_stats, on='id', how='left')

    # Step 7: Community data
    community_stats = query_communities(user_ids)
    if community_stats:
        df_community = pd.DataFrame(community_stats).rename(columns={'user_id': 'id'})
        df_community['feature_usage_in_community'] = df_community['community_count'].apply(
            lambda x: 'yes' if x > 0 else 'no'
        )
        df_cleaned = df_cleaned.merge(df_community[['id', 'feature_usage_in_community']], on='id', how='left')
        df_cleaned['feature_usage_in_community'] = df_cleaned['feature_usage_in_community'].fillna('no')

    # Step 8: Batches created
    batches_created = query_batches_created_by_user(user_ids)
    if batches_created:
        df_batches = pd.DataFrame(batches_created).rename(columns={'user_id': 'id'})
        df_cleaned = df_cleaned.merge(df_batches, on='id', how='left')
        df_cleaned['batches_created_by_user'] = df_cleaned['batches_created_by_user'].fillna(0).astype(int)

    # Step 9: Final mapping
    df_final = create_final_mapping(df_sheet, df_cleaned, year)

    print(f"\n📋 Final Results (head 10) for {year}:")
    print("=" * 120)
    print(df_final.head(10).to_string())
    print("=" * 120)


# =========================
# MAIN
# =========================
if __name__ == "__main__":
    print("🚀 Starting Mobile/Email Lookup Pipeline (3-year data)...\n")

    for sheet_cfg in SHEETS:
        process_sheet(sheet_cfg)

    print("\n✨ Pipeline completed for all years!")

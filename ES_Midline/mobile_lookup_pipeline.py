# Mobile Number Lookup Pipeline
# Reads Google Sheets CSV, extracts mobile numbers, and queries source database

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
GOOGLE_SHEETS_CSV_URL = "https://docs.google.com/spreadsheets/d/e/2PACX-1vTMLcox2ea1Pn-pP2fQWHgeo2wZqrUK07FyvFAfnZ12H2YUEC0b3_NOkiCE81WV1Lykq-d1qCv8GjeU/pub?gid=2095527202&single=true&output=csv"
MOBILE_COLUMN = "_2_Mobile_Number"
BASE_DIR = Path(__file__).resolve().parent


def get_absolute_key_path(relative_path):
    """Convert relative SSH key path to absolute path."""
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
        # Create SSL context that bypasses verification for public Google Sheets (macOS SSL fix)
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE
        
        # Fetch URL with SSL bypass
        with urllib.request.urlopen(url, context=ssl_context) as response:
            df = pd.read_csv(response)
        
        print(f"✅ Successfully read CSV ({len(df)} rows)")
        return df
    except Exception as e:
        print(f"❌ Error fetching CSV: {e}")
        return None


def extract_mobile_numbers(df, column_name):
    """Extract 10-digit mobile numbers from dataframe column."""
    print(f"\n🔍 Extracting mobile numbers from '{column_name}'...")
    
    if column_name not in df.columns:
        print(f"⚠️ Column '{column_name}' not found. Available columns:")
        print(df.columns.tolist())
        return [], None
    
    mobile_df = df[column_name].astype(str).str.strip()
    
    # Filter for 10-digit numbers only
    valid_mobiles = mobile_df[mobile_df.str.match(r'^\d{10}$')]
    mobile_numbers = valid_mobiles.unique().tolist()
    
    # Create dataframe with unique mobile numbers from Google Sheets
    df_sheet_mobiles = pd.DataFrame({'mobile': mobile_numbers})
    
    print(f"📊 Found {len(mobile_numbers)} unique 10-digit mobile numbers")
    print(f"   (Total rows: {len(df)}, matches: {len(mobile_numbers)})")
    
    return mobile_numbers, df_sheet_mobiles


def query_batch_statistics(user_ids):
    """Query batch statistics for users."""
    
    if not user_ids:
        print("❌ No user IDs provided")
        return None
    
    print(f"\n📦 Fetching batch statistics for {len(user_ids)} users...")
    
    source_cfg = config.CONFIG["source"]
    ssh_key_path = get_absolute_key_path(source_cfg["ssh"]["pkey_path"])
    
    # Build placeholders for SQL IN clause
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
        print("❌ No user IDs provided")
        return None
    
    print(f"\n🔐 Fetching login logs for {len(user_ids)} users...")
    
    source_cfg = config.CONFIG["source"]
    ssh_key_path = get_absolute_key_path(source_cfg["ssh"]["pkey_path"])
    
    # Build placeholders for SQL IN clause
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
        print("❌ No user IDs provided")
        return None
    
    print(f"\n🏘️ Fetching community data for {len(user_ids)} users...")
    source_cfg = config.CONFIG["source"]
    ssh_key_path = get_absolute_key_path(source_cfg["ssh"]["pkey_path"])
    
    # Build placeholders for SQL IN clause
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
        print("❌ No user IDs provided")
        return None
    
    print(f"\n📦 Fetching batches created by {len(user_ids)} users...")
    source_cfg = config.CONFIG["source"]
    ssh_key_path = get_absolute_key_path(source_cfg["ssh"]["pkey_path"])
    
    # Build placeholders for SQL IN clause
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


def query_users_by_mobile(mobile_numbers):
    """Query source database for users matching mobile numbers."""
    
    if not mobile_numbers:
        print("❌ No mobile numbers to search")
        return None
    
    print(f"\n📡 Querying source database for {len(mobile_numbers)} mobile numbers...")
    print(f"🕒 Started at: {datetime.now()}")
    
    source_cfg = config.CONFIG["source"]
    
    # Convert relative SSH key path to absolute path
    ssh_key_path = get_absolute_key_path(source_cfg["ssh"]["pkey_path"])
    
    # Build placeholders for SQL IN clause
    placeholders = ",".join(f"'{m}'" for m in mobile_numbers)
    
    query = f"""
    SELECT
        u.id,
        u.mobile,
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
        u.mobile IN ({placeholders})
    ORDER BY u.id
    """
    
    try:
        # Connect via SSH tunnel to source database
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


def display_results(results, head_count=5):
    """Display results as DataFrame."""
    if not results:
        print("⚠️ No results to display")
        return None
    
    df_results = pd.DataFrame(results)
    
    print(f"\n📋 Results (head({head_count})):")
    print("=" * 80)
    print(df_results.head(head_count).to_string())
    print("=" * 80)
    print(f"\n📊 Summary: {len(df_results)} total users found")
    
    return df_results


def clean_and_deduplicate_users(df_results):
    """Clean and deduplicate users based on mobile number with specific filtering rules."""
    print(f"\n🧹 Starting deduplication process...")
    print(f"📊 Initial records: {len(df_results)}")
    
    # Save raw results first
    raw_csv_path = BASE_DIR / "mobile_lookup_raw_results.csv"
    df_results.to_csv(raw_csv_path, index=False)
    print(f"💾 Raw results saved to: {raw_csv_path}")
    
    # Convert deleted_at to datetime and handle NaT
    df_results['deleted_at'] = pd.to_datetime(df_results['deleted_at'], errors='coerce')
    
    # Type priority mapping
    type_priority = {1: 1, 2: 2, 3: 3, 4: 4}
    
    # Group by mobile number
    grouped = df_results.groupby('mobile')
    cleaned_records = []
    
    for mobile, group in grouped:
        records = group.copy()
        
        # If only one record, keep it
        if len(records) == 1:
            cleaned_records.append(records.iloc[0])
            continue
        
        print(f"📱 Processing mobile {mobile}: {len(records)} records")
        
        # Filter 1: Remove records with deleted_at (not null)
        active_records = records[records['deleted_at'].isna()]
        if len(active_records) == 1:
            cleaned_records.append(active_records.iloc[0])
            continue
        
        # If no active records, prioritize by type, then by most recent deleted
        if len(active_records) == 0:
            records_copy = records.copy()
            records_copy['type_rank'] = records_copy['type'].map(type_priority).fillna(999)
            records_copy = records_copy.sort_values(['type_rank', 'deleted_at'], ascending=[True, False])
            cleaned_records.append(records_copy.iloc[0])
            continue
        
        # Filter 2: Keep only status = 1
        status_active = active_records[active_records['status'] == 1]
        if len(status_active) == 1:
            cleaned_records.append(status_active.iloc[0])
            continue
        
        # If no status=1 records, keep first active record
        if len(status_active) == 0:
            cleaned_records.append(active_records.iloc[0])
            continue
        
        # Filter 3: Prefer types in priority order 1, 2, 3, 4
        status_active = status_active.copy()
        status_active['type_rank'] = status_active['type'].map(type_priority).fillna(999)
        status_active = status_active.sort_values(['type_rank'], ascending=[True])
        cleaned_records.append(status_active.iloc[0])
    
    # Create cleaned dataframe
    df_cleaned = pd.DataFrame(cleaned_records)
    
    # Remove the type_rank column if it exists
    if 'type_rank' in df_cleaned.columns:
        df_cleaned = df_cleaned.drop('type_rank', axis=1)
    
    # Save cleaned results
    cleaned_csv_path = BASE_DIR / "mobile_lookup_cleaned_results.csv"
    df_cleaned.to_csv(cleaned_csv_path, index=False)
    
    print(f"\n✅ Deduplication completed!")
    print(f"📊 Final unique records: {len(df_cleaned)}")
    print(f"💾 Cleaned results saved to: {cleaned_csv_path}")
    
    return df_cleaned


def create_final_mapping(df_sheet_mobiles, df_cleaned):
    """Create final dataframe with left join and InQuestApp flag."""
    print(f"\n🔗 Creating final mapping with Google Sheets mobile numbers...")
    
    # Reset index of cleaned dataframe to avoid index issues
    df_cleaned_reset = df_cleaned.reset_index(drop=True)
    
    # Left join: all mobiles from Google Sheets with cleaned data
    df_final = df_sheet_mobiles.merge(
        df_cleaned_reset,
        on='mobile',
        how='left'
    )
    
    # Add InQuestApp column: 1 if in cleaned data (id is not null), 0 otherwise
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
    
    # Reorder columns to put InQuestApp, user_valid_status, batches_created_by_user, and feature_usage_in_community at the end
    cols = [col for col in df_final.columns if col not in ['InQuestApp', 'user_valid_status', 'batches_created_by_user', 'feature_usage_in_community']] + ['InQuestApp', 'user_valid_status', 'batches_created_by_user', 'feature_usage_in_community']
    df_final = df_final[cols]
    
    # Save final results
    final_csv_path = BASE_DIR / "mobile_lookup_final_results.csv"
    df_final.to_csv(final_csv_path, index=False)
    
    in_app_count = (df_final['InQuestApp'] == 1).sum()
    not_in_app_count = (df_final['InQuestApp'] == 0).sum()
    
    print(f"✅ Mapping completed!")
    print(f"📊 Total mobile numbers: {len(df_final)}")
    print(f"   ✓ In Quest App: {in_app_count}")
    print(f"   ✗ Not in Quest App: {not_in_app_count}")
    print(f"💾 Final results saved to: {final_csv_path}")
    
    return df_final


# =========================
# MAIN
# =========================
if __name__ == "__main__":
    print("🚀 Starting Mobile Number Lookup Pipeline...\n")
    
    # Step 1: Fetch Google Sheets CSV
    df_sheets = fetch_google_sheets_csv(GOOGLE_SHEETS_CSV_URL)
    if df_sheets is None:
        exit(1)
    
    # Step 2: Extract 10-digit mobile numbers
    mobile_numbers, df_sheet_mobiles = extract_mobile_numbers(df_sheets, MOBILE_COLUMN)
    if not mobile_numbers:
        print("\n⚠️ No mobile numbers found to process")
        exit(1)
    
    # Step 3: Query source database
    query_results = query_users_by_mobile(mobile_numbers)
    if query_results is None:
        exit(1)
    
    # Step 4: Display raw results
    df_raw = display_results(query_results, head_count=5)
    
    # Step 5: Clean and deduplicate
    if df_raw is not None:
        df_cleaned = clean_and_deduplicate_users(df_raw)
        
        # Step 6: Fetch batch statistics for cleaned users
        user_ids = df_cleaned['id'].unique().tolist()
        batch_stats = query_batch_statistics(user_ids)
        
        if batch_stats:
            df_batch_stats = pd.DataFrame(batch_stats)
            df_cleaned = df_cleaned.merge(df_batch_stats, on='centre_id', how='left')
        
        # Step 7: Fetch login logs for cleaned users
        login_stats = query_login_logs(user_ids)
        
        if login_stats:
            df_login_stats = pd.DataFrame(login_stats)
            df_login_stats = df_login_stats.rename(columns={'user_id': 'id'})
            df_cleaned = df_cleaned.merge(df_login_stats, on='id', how='left')
        
        # Step 8: Fetch community data for cleaned users
        community_stats = query_communities(user_ids)
        
        if community_stats:
            df_community_stats = pd.DataFrame(community_stats)
            df_community_stats = df_community_stats.rename(columns={'user_id': 'id'})
            # Add feature_usage_in_community column: 'yes' if community_count > 0, 'no' otherwise
            df_community_stats['feature_usage_in_community'] = df_community_stats['community_count'].apply(lambda x: 'yes' if x > 0 else 'no')
            df_community_stats = df_community_stats[['id', 'feature_usage_in_community']]
            df_cleaned = df_cleaned.merge(df_community_stats, on='id', how='left')
            # Fill NaN values with 'no' for users without community records
            df_cleaned['feature_usage_in_community'] = df_cleaned['feature_usage_in_community'].fillna('no')
        
        # Step 9: Fetch batches created by users
        batches_created_stats = query_batches_created_by_user(user_ids)
        
        if batches_created_stats:
            df_batches_created_stats = pd.DataFrame(batches_created_stats)
            df_batches_created_stats = df_batches_created_stats.rename(columns={'user_id': 'id'})
            df_cleaned = df_cleaned.merge(df_batches_created_stats, on='id', how='left')
            # Fill NaN values with 0 for users who haven't created any batches
            df_cleaned['batches_created_by_user'] = df_cleaned['batches_created_by_user'].fillna(0).astype(int)
        print(f"\n📋 Cleaned Results (head(5)):")
        print("=" * 120)
        print(df_cleaned.head(5).to_string())
        print("=" * 120)
        print(f"\n📊 Cleaned Summary: {len(df_cleaned)} unique users")
        
        # Step 8: Create final mapping with InQuestApp flag
        df_final = create_final_mapping(df_sheet_mobiles, df_cleaned)
        
        # Display final results
        print(f"\n📋 Final Results with Mapping (head(10)):")
        print("=" * 120)
        print(df_final.head(10).to_string())
        print("=" * 120)
    
    print("\n✨ Mobile lookup pipeline completed successfully!")

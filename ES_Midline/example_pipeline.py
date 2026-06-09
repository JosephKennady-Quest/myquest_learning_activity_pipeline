# Reads SQL queries from prod and writes to analytics DB
# STREAMING + BATCH-AWARE (for large fact tables)
# SQL-safe | CTE-safe | SSH-stable | Superset-safe

import pandas as pd
import numpy as np
import pymysql
from sshtunnel import SSHTunnelForwarder
from pathlib import Path
from datetime import datetime
import DONTUPLOADTOGITconfig as config


# =========================
# CONFIG
# =========================
CHUNK_SIZE = config.CHUNK_SIZE
USER_BATCH_SIZE = 300   # 🔑 critical for large tables


# =========================
# HELPERS
# =========================
def batched(iterable, size):
    for i in range(0, len(iterable), size):
        yield iterable[i:i + size]


# =========================
# CORE FUNCTION
# =========================
def run_query_and_transfer(file_path):
    table_name = file_path.stem
    print(f"\n📂 Processing: {file_path.name} → {table_name}")
    print(f"🕒 Started at: {datetime.now()}")

    # ---- Read SQL
    with open(file_path, "r") as f:
        raw_query = f.read()

    clean_query = "\n".join(
        line for line in raw_query.splitlines()
        if not line.strip().startswith("--")
    ).strip()

    if not clean_query.lower().startswith(("select", "with")):
        print(f"⚠️ Skipping {file_path.name} (not SELECT / WITH)")
        return

    is_batched = "{placeholders}" in clean_query

    # Access new CONFIG structure
    source_cfg = config.CONFIG["source"]
    dest_cfg = config.CONFIG["destination"]

    # =====================
    # SOURCE SSH + DB
    # =====================
    with SSHTunnelForwarder(
        (source_cfg["ssh"]["host"], source_cfg["ssh"].get("port", 22)),
        ssh_username=source_cfg["ssh"]["username"],
        ssh_pkey=source_cfg["ssh"]["pkey_path"],
        remote_bind_address=(source_cfg["ssh"]["remote_bind_address"], source_cfg["ssh"]["remote_bind_port"]),
        set_keepalive=30
    ) as app_tunnel:

        app_conn = pymysql.connect(
            host="127.0.0.1",
            port=app_tunnel.local_bind_port,
            user=source_cfg["db"]["user"],
            password=source_cfg["db"]["password"],
            database=source_cfg["db"]["database"],
            cursorclass=pymysql.cursors.SSCursor,
            connect_timeout=60,
            read_timeout=600,
            write_timeout=600,
            autocommit=False
        )

        # =====================
        # DEST SSH + DB
        # =====================
        with SSHTunnelForwarder(
            (dest_cfg["ssh"]["host"], dest_cfg["ssh"].get("port", 22)),
            ssh_username=dest_cfg["ssh"]["username"],
            ssh_pkey=dest_cfg["ssh"]["pkey_path"],
            remote_bind_address=(dest_cfg["ssh"]["remote_bind_address"], dest_cfg["ssh"]["remote_bind_port"]),
            set_keepalive=30
        ) as analytics_tunnel:

            analytics_conn = pymysql.connect(
                host="127.0.0.1",
                port=analytics_tunnel.local_bind_port,
                user=dest_cfg["db"]["user"],
                password=dest_cfg["db"]["password"],
                database=dest_cfg["db"]["database"],
                cursorclass=pymysql.cursors.DictCursor,
                autocommit=False
            )

            source_cursor = app_conn.cursor()
            dest_cursor = analytics_conn.cursor()

            try:
                # =====================
                # GET USER BATCHES (if needed)
                # =====================
                if is_batched:
                    print("🔍 Batched SQL detected — fetching user IDs")
                    source_cursor.execute("""
                        SELECT id
                        FROM users
                        WHERE is_ple = 1 AND deleted_at IS NULL AND status = 1
                        ORDER BY id
                    """)
                    user_ids = [row[0] for row in source_cursor.fetchall()]
                    user_batches = list(batched(user_ids, USER_BATCH_SIZE))
                    print(f"👥 {len(user_ids)} users → {len(user_batches)} batches")
                else:
                    user_batches = [None]

                first_write = True
                total_rows = 0

                # =====================
                # EXECUTE PER BATCH
                # =====================
                for batch_idx, batch in enumerate(user_batches, start=1):
                    if batch:
                        placeholders = ",".join(f"'{u}'" for u in batch)
                        query = clean_query.replace("{placeholders}", placeholders)
                        print(f"📦 Running batch {batch_idx}/{len(user_batches)}")
                    else:
                        query = clean_query

                    source_cursor.execute(query)
                    column_names = [desc[0] for desc in source_cursor.description]

                    while True:
                        rows = source_cursor.fetchmany(CHUNK_SIZE)
                        if not rows:
                            break

                        df = pd.DataFrame(rows, columns=column_names)
                        if df.empty:
                            continue

                        if first_write:
                            cols = ", ".join(f"`{c}` TEXT" for c in df.columns)
                            dest_cursor.execute(f"DROP TABLE IF EXISTS `{table_name}`")
                            dest_cursor.execute(f"CREATE TABLE `{table_name}` ({cols})")
                            first_write = False

                        df.replace({np.nan: None}, inplace=True)

                        insert_sql = f"""
                            INSERT INTO `{table_name}`
                            ({", ".join(f"`{c}`" for c in df.columns)})
                            VALUES ({", ".join(["%s"] * len(df.columns))})
                        """

                        dest_cursor.executemany(insert_sql, df.values.tolist())
                        analytics_conn.commit()

                        total_rows += len(df)

                print(f"✅ Completed {file_path.name} ({total_rows} rows)")
                print(f"🕓 Ended at: {datetime.now()}")

            finally:
                source_cursor.close()
                dest_cursor.close()
                app_conn.close()
                analytics_conn.close()


# =========================
# MAIN
# =========================
if __name__ == "__main__":
    print("🚀 Starting SQL pipeline...\n")

    BASE_DIR = Path(__file__).resolve().parent
    for sql_file in BASE_DIR.glob("*.sql"):
        run_query_and_transfer(sql_file)

    print("\n🎉 All files processed.")

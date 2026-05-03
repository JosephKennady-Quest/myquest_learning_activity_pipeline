import os
from dotenv import load_dotenv

load_dotenv()

# DB_Config folder lives inside this pipeline folder
# ael_v2_pipeline/DB_Config/
_PIPELINE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_CONFIG_DIR = os.path.join(_PIPELINE_DIR, "DB_Config")

CONFIG = {
    "source": {
        "ssh": {
            "host":                os.getenv("SOURCE_SSH_HOST"),
            "port":                int(os.getenv("SOURCE_SSH_PORT", "22")),
            "username":            os.getenv("SOURCE_SSH_USER"),
            "pkey_path":           os.path.join(DB_CONFIG_DIR, os.getenv("SOURCE_SSH_PKEY_FILE", "")),
            "remote_bind_address": os.getenv("SOURCE_RDS_HOST"),
            "remote_bind_port":    int(os.getenv("SOURCE_RDS_PORT", "3306")),
        },
        "db": {
            "user":     os.getenv("SOURCE_DB_USER"),
            "password": os.getenv("SOURCE_DB_PASSWORD"),
            "database": os.getenv("SOURCE_DB_NAME", "quest_rearch_production"),
        },
    },
    "destination": {
        "ssh": {
            "host":                os.getenv("DEST_SSH_HOST"),
            "port":                int(os.getenv("DEST_SSH_PORT", "22")),
            "username":            os.getenv("DEST_SSH_USER"),
            "pkey_path":           os.path.join(DB_CONFIG_DIR, os.getenv("DEST_SSH_PKEY_FILE", "")),
            "remote_bind_address": os.getenv("DEST_RDS_HOST"),
            "remote_bind_port":    int(os.getenv("DEST_RDS_PORT", "3306")),
        },
        "db": {
            "user":     os.getenv("DEST_DB_USER"),
            "password": os.getenv("DEST_DB_PASSWORD"),
            "database": os.getenv("DEST_DB_NAME", "quest_ple_analytics"),
        },
    },
}

# ── Aliases used by all pipeline steps ────────────────────────────────────────
SOURCE_DB    = CONFIG["source"]
ANALYTICS_DB = CONFIG["destination"]

# ── Pipeline constants ────────────────────────────────────────────────────────
CHUNK_SIZE             = 5000   # DB insert batch size (rows per executemany call)
ALLOC_CHUNK_SIZE       = 2000   # learner users per allocation query (prevents OOM on full runs)
STAFF_ALLOC_CHUNK_SIZE = 200    # staff per allocation query (admins get many more rows)
LEARNER_TYPES     = (3, 4)
LEARNER_TYPES_SQL = ",".join(str(t) for t in LEARNER_TYPES)

STAFF_TYPES       = (1, 2)          # Admin (1), Facilitator / Master Trainer (2)
STAFF_TYPES_SQL   = ",".join(str(t) for t in STAFF_TYPES)

ALL_TYPES         = STAFF_TYPES + LEARNER_TYPES   # (1, 2, 3, 4)
ALL_TYPES_SQL     = ",".join(str(t) for t in ALL_TYPES)
OUTPUT_DIR        = os.getenv("OUTPUT_DIR", "output")

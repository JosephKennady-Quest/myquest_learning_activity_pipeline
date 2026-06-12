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
ALLOC_CHUNK_SIZE       = 2000   # learner users per allocation query
STAFF_ALLOC_CHUNK_SIZE = 200    # staff users per allocation query; admins expand to all centre lessons

LEARNER_TYPES     = (3, 4)
LEARNER_TYPES_SQL = ",".join(str(t) for t in LEARNER_TYPES)

STAFF_TYPES       = (1, 2)          # Admin (1), Facilitator / Master Trainer (2)
STAFF_TYPES_SQL   = ",".join(str(t) for t in STAFF_TYPES)

ALL_TYPES         = STAFF_TYPES + LEARNER_TYPES   # (1, 2, 3, 4)
ALL_TYPES_SQL     = ",".join(str(t) for t in ALL_TYPES)
OUTPUT_DIR        = os.getenv("OUTPUT_DIR", "output")

# ── Parallelism ───────────────────────────────────────────────────────────────
# Number of worker threads for concurrent chunk processing.
# Each worker holds one DuckDB read connection + one SSH tunnel for completion.
# Rule of thumb: 4 works well on a 4-core machine; raise to 8 on 8+ cores.
# Set to 1 to disable parallelism (equivalent to the original sequential loop).
CHUNK_WORKERS = int(os.getenv("CHUNK_WORKERS", "4"))

# ── Direct DB connection (no SSH tunnel) ──────────────────────────────────────
# On a host with direct network access to the RDS endpoints (e.g. the server
# inside the VPC), set DB_DIRECT=1 to connect pymysql straight to
# SOURCE_RDS_HOST / DEST_RDS_HOST and skip the SSH tunnel entirely.  This is
# faster (no SSH forwarding overhead) and naturally parallel — each worker gets
# its own independent MySQL connection instead of sharing one SSH transport.
# Leave unset (default) on a laptop that must reach RDS through the bastion.
DB_DIRECT = os.getenv("DB_DIRECT", "false").strip().lower() in ("1", "true", "yes", "on")

# ── DuckDB memory cap ─────────────────────────────────────────────────────────
# By default DuckDB sizes its buffer pool at ~80% of system RAM. During a full
# run it holds the source + completion tables AND a growing allocation_cache,
# which leaves no headroom for the in-memory completion DataFrame and the
# parallel workers' per-chunk frames — causing the OS to OOM-kill the process.
# Cap it so DuckDB spills to disk instead of competing with pandas for RAM.
DUCKDB_MEMORY_LIMIT = os.getenv("DUCKDB_MEMORY_LIMIT", "10GB")

# ── Cache invalidation ────────────────────────────────────────────────────────
# Strategy used to detect whether allocation source tables changed.
#   "hash"      — CRC32 of sorted (id, updated_at) values; more precise,
#                 no false invalidations from unrelated row inserts.
#   "row_count" — original behaviour; cheaper but may trigger unnecessary refreshes.
CACHE_INVALIDATION_STRATEGY = os.getenv("CACHE_INVALIDATION_STRATEGY", "hash")

"""
db_connection.py — SSH tunnel + MySQL connection template
Works on macOS, Linux, and Windows (Python 3.8+)
"""

import os
import sys
import platform
import stat
import logging
from pathlib import Path
from contextlib import contextmanager

import paramiko
import pymysql
from sshtunnel import SSHTunnelForwarder
from dotenv import load_dotenv

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Load .env ─────────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")

DB_CONFIG_DIR = BASE_DIR / "DB_Config"


def _pem_path(filename: str) -> Path:
    """Return the absolute path to a PEM file and fix permissions on Unix."""
    path = DB_CONFIG_DIR / filename
    if not path.exists():
        raise FileNotFoundError(f"PEM file not found: {path}")

    # On macOS / Linux SSH requires the key to be chmod 400/600
    if platform.system() != "Windows":
        current = stat.S_IMODE(path.stat().st_mode)
        if current & 0o177:  # any bits beyond owner-read are set
            log.info("Fixing permissions on %s → 0o400", path.name)
            path.chmod(0o400)

    return path


def _get_env(key: str) -> str:
    value = os.getenv(key)
    if not value:
        raise EnvironmentError(f"Missing required environment variable: {key}")
    return value


# ── Tunnel + Connection helpers ───────────────────────────────────────────────

def _load_pkey(pem: Path):
    """Try each key type in turn — handles RSA, Ed25519, ECDSA, and OpenSSH format."""
    for key_class in (
        paramiko.Ed25519Key,
        paramiko.RSAKey,
        paramiko.ECDSAKey,
    ):
        try:
            return key_class.from_private_key_file(str(pem))
        except paramiko.SSHException:
            continue
    raise paramiko.SSHException(f"Unable to load private key: {pem}")


@contextmanager
def ssh_tunnel(
    ssh_host: str,
    ssh_port: int,
    ssh_user: str,
    pem_file: str,
    rds_host: str,
    rds_port: int,
):
    """Context manager that opens an SSH tunnel and yields the local port."""
    pem = _pem_path(pem_file)
    pkey = _load_pkey(pem)

    log.info("Opening SSH tunnel  %s@%s:%s  →  %s:%s", ssh_user, ssh_host, ssh_port, rds_host, rds_port)

    server = SSHTunnelForwarder(
        (ssh_host, ssh_port),
        ssh_username=ssh_user,
        ssh_pkey=pkey,
        remote_bind_address=(rds_host, rds_port),
    )
    server.start()
    log.info("Tunnel open on local port %s", server.local_bind_port)
    try:
        yield server.local_bind_port
    finally:
        server.stop()
        log.info("Tunnel closed")


@contextmanager
def mysql_connection(local_port: int, db_user: str, db_password: str, db_name: str):
    """Context manager that yields an open pymysql connection."""
    conn = pymysql.connect(
        host="127.0.0.1",
        port=local_port,
        user=db_user,
        password=db_password,
        database=db_name,
        connect_timeout=10,
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
    )
    log.info("MySQL connected  →  %s", db_name)
    try:
        yield conn
    finally:
        conn.close()
        log.info("MySQL connection closed")


# ── Public convenience functions ──────────────────────────────────────────────

@contextmanager
def db_qapp_production():
    """Open a connection to the Source DB (quest_rearch_production)."""
    with ssh_tunnel(
        ssh_host=_get_env("SOURCE_SSH_HOST"),
        ssh_port=int(_get_env("SOURCE_SSH_PORT")),
        ssh_user=_get_env("SOURCE_SSH_USER"),
        pem_file=_get_env("SOURCE_SSH_PKEY_FILE"),
        rds_host=_get_env("SOURCE_RDS_HOST"),
        rds_port=int(_get_env("SOURCE_RDS_PORT")),
    ) as port:
        with mysql_connection(
            local_port=port,
            db_user=_get_env("SOURCE_DB_USER"),
            db_password=_get_env("SOURCE_DB_PASSWORD"),
            db_name=_get_env("SOURCE_DB_NAME"),
        ) as conn:
            yield conn


@contextmanager
def db_analytics():
    """Open a connection to the Destination DB (analytics_)."""
    with ssh_tunnel(
        ssh_host=_get_env("DEST_SSH_HOST"),
        ssh_port=int(_get_env("DEST_SSH_PORT")),
        ssh_user=_get_env("DEST_SSH_USER"),
        pem_file=_get_env("DEST_SSH_PKEY_FILE"),
        rds_host=_get_env("DEST_RDS_HOST"),
        rds_port=int(_get_env("DEST_RDS_PORT")),
    ) as port:
        with mysql_connection(
            local_port=port,
            db_user=_get_env("DEST_DB_USER"),
            db_password=_get_env("DEST_DB_PASSWORD"),
            db_name=_get_env("DEST_DB_NAME"),
        ) as conn:
            yield conn


# ── Quick smoke-test ───────────────────────────────────────────────────────────

def test_connections():
    log.info("=== Testing SOURCE DB ===")
    with db_qapp_production() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT DATABASE() AS db, NOW() AS ts")
            row = cur.fetchone()
            log.info("Source DB: %s", row)

    log.info("=== Testing DESTINATION DB ===")
    with db_analytics() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT DATABASE() AS db, NOW() AS ts")
            row = cur.fetchone()
            log.info("Dest DB:   %s", row)

    log.info("All connections OK.")


if __name__ == "__main__":
    test_connections()

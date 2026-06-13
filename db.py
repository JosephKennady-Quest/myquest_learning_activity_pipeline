"""
db.py — SSH tunnel management, MySQL helpers, and connection pooling.

Optimisations vs original:
  • TunnelPool  — one persistent SSH tunnel per DB config, reused across all
                  fetch() / write_table() calls in a run.  Eliminates the
                  ~1,270 tunnel open/close cycles in a 635-chunk run.
  • write_table_persistent — accepts a live pymysql connection so the caller
                  can keep one SSH tunnel open for the entire flush loop
                  instead of re-opening per stream batch.
  • write_table  — unchanged public API; internally uses TunnelPool when one
                  is active for the target config, falls back to a fresh
                  _tunnel() otherwise.
"""

import hashlib
import logging
import select
import socket
import threading
from contextlib import contextmanager
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import paramiko
import pymysql

from config import CHUNK_SIZE, DB_DIRECT

log = logging.getLogger(__name__)


def _direct_connect(cfg: Dict[str, Any]) -> "pymysql.connections.Connection":
    """
    Connect pymysql straight to the RDS endpoint, no SSH tunnel.

    Used when DB_DIRECT is set (host has direct network access to RDS).
    The RDS host/port live in cfg['ssh']['remote_bind_address'/'remote_bind_port'].
    Each call returns an independent connection, so worker threads run fully
    in parallel with no shared transport.
    """
    return pymysql.connect(
        host=cfg["ssh"]["remote_bind_address"],
        port=int(cfg["ssh"]["remote_bind_port"]),
        user=cfg["db"]["user"],
        password=cfg["db"]["password"],
        database=cfg["db"]["database"],
        charset="utf8mb4",
        connect_timeout=30,
    )


# ─────────────────────────────────────────────────────────────────────────────
# Low-level port-forwarding helpers (unchanged)
# ─────────────────────────────────────────────────────────────────────────────

def _bridge(local_sock: socket.socket, channel: paramiko.Channel) -> None:
    """Bidirectional copy between a local TCP socket and a paramiko channel."""
    while True:
        try:
            r, _, x = select.select([local_sock, channel], [], [local_sock, channel], 1.0)
            if x:
                break
            if local_sock in r:
                data = local_sock.recv(4096)
                if not data:
                    break
                channel.sendall(data)
            if channel in r:
                data = channel.recv(4096)
                if not data:
                    break
                local_sock.sendall(data)
        except Exception:
            break
    try:
        local_sock.close()
    except Exception:
        pass
    try:
        channel.close()
    except Exception:
        pass


@contextmanager
def _tunnel(ssh: Dict[str, Any]):
    """
    Open an SSH connection and spin up a local TCP server that forwards
    each accepted connection to the RDS endpoint via a direct-tcpip channel.

    Yields an object with a local_bind_port attribute so callers can connect
    pymysql to 127.0.0.1:<local_bind_port>.
    """
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(
        hostname=ssh["host"],
        port=ssh["port"],
        username=ssh["username"],
        key_filename=ssh["pkey_path"],
    )
    log.debug("SSH connected → %s@%s", ssh["username"], ssh["host"])

    transport = client.get_transport()

    server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_sock.bind(("127.0.0.1", 0))
    local_port = server_sock.getsockname()[1]
    server_sock.listen(5)
    server_sock.settimeout(1.0)

    stop = threading.Event()

    def _serve():
        while not stop.is_set():
            try:
                conn_sock, _ = server_sock.accept()
            except socket.timeout:
                continue
            except Exception:
                break
            try:
                ch = transport.open_channel(
                    "direct-tcpip",
                    (ssh["remote_bind_address"], ssh["remote_bind_port"]),
                    ("127.0.0.1", 0),
                )
            except Exception as e:
                log.warning("Could not open channel: %s", e)
                conn_sock.close()
                continue
            threading.Thread(target=_bridge, args=(conn_sock, ch), daemon=True).start()

    threading.Thread(target=_serve, daemon=True).start()
    log.debug(
        "Local forwarder listening on 127.0.0.1:%d → %s:%d",
        local_port, ssh["remote_bind_address"], ssh["remote_bind_port"],
    )

    class _Tunnel:
        local_bind_port = local_port

    try:
        yield _Tunnel()
    finally:
        stop.set()
        server_sock.close()
        client.close()
        log.debug("SSH disconnected ← %s", ssh["host"])


@contextmanager
def _connect(cfg: Dict[str, Any]):
    """
    Open a fresh SSH tunnel, then connect pymysql to the forwarded local port.
    Used as the fallback when no TunnelPool is active.
    """
    with _tunnel(cfg["ssh"]) as tunnel:
        conn = pymysql.connect(
            host="127.0.0.1",
            port=tunnel.local_bind_port,
            user=cfg["db"]["user"],
            password=cfg["db"]["password"],
            database=cfg["db"]["database"],
            charset="utf8mb4",
        )
        try:
            yield conn
        finally:
            conn.close()


# ─────────────────────────────────────────────────────────────────────────────
# Opt 3 — SSH Tunnel Pool
# ─────────────────────────────────────────────────────────────────────────────

class TunnelPool:
    """
    Persistent SSH tunnel pool — one long-lived tunnel per DB config.

    Usage (in main.py, wrapping the entire run):

        with TunnelPool() as pool:
            pool.open(SOURCE_DB)
            pool.open(ANALYTICS_DB)
            # All fetch() / write_table() calls now reuse these tunnels.

    Thread safety:
        Multiple worker threads may call _get_conn() concurrently.
        Each call acquires a fresh pymysql connection from the shared tunnel
        (the SSH transport supports multiple channels).  A per-config lock
        guards tunnel re-establishment if a connection is lost.

    Fallback:
        If the pool is not active (i.e. no TunnelPool context is in scope),
        fetch() and write_table() fall back to the original _connect() path.
    """

    # Module-level singleton: active pool, or None when no pool is in scope.
    _active: "TunnelPool | None" = None

    def __init__(self):
        # key = _cfg_key(cfg); value = {"port": int, "ssh_client": paramiko.SSHClient,
        #                               "server_sock": socket, "stop": Event}
        self._tunnels: Dict[str, Dict[str, Any]] = {}
        self._locks:   Dict[str, threading.Lock] = {}
        self._cfg_map: Dict[str, Dict[str, Any]] = {}   # key → full cfg dict

    # ── Context manager ───────────────────────────────────────────────────────

    def __enter__(self):
        TunnelPool._active = self
        return self

    def __exit__(self, *_):
        self.close_all()
        TunnelPool._active = None

    # ── Public API ────────────────────────────────────────────────────────────

    def open(self, cfg: Dict[str, Any]) -> None:
        """
        Open a persistent SSH tunnel for cfg and keep it alive.
        Safe to call multiple times for the same cfg (idempotent).
        No-op in DB_DIRECT mode (connections bypass the tunnel).
        """
        if DB_DIRECT:
            log.info(
                "[TunnelPool] DB_DIRECT mode — connecting straight to %s:%s (no SSH tunnel)",
                cfg["ssh"]["remote_bind_address"], cfg["ssh"]["remote_bind_port"],
            )
            return
        key = _cfg_key(cfg)
        if key in self._tunnels:
            return
        self._locks.setdefault(key, threading.Lock())
        self._cfg_map[key] = cfg
        self._establish(key)
        log.info(
            "[TunnelPool] opened tunnel → %s:%d  (local port %d)",
            cfg["ssh"]["remote_bind_address"],
            cfg["ssh"]["remote_bind_port"],
            self._tunnels[key]["port"],
        )

    def get_conn(self, cfg: Dict[str, Any]) -> pymysql.connections.Connection:
        """
        Return a fresh pymysql connection through the pooled tunnel for cfg.
        Opens the tunnel if not already open.
        Re-establishes the tunnel automatically on connection failure.
        Caller is responsible for closing the connection when done.
        """
        key = _cfg_key(cfg)
        if key not in self._tunnels:
            self.open(cfg)

        with self._locks[key]:
            port = self._tunnels[key]["port"]
            try:
                conn = pymysql.connect(
                    host="127.0.0.1",
                    port=port,
                    user=cfg["db"]["user"],
                    password=cfg["db"]["password"],
                    database=cfg["db"]["database"],
                    charset="utf8mb4",
                    connect_timeout=30,
                )
                # Verify the connection is alive
                conn.ping(reconnect=False)
                return conn
            except Exception as exc:
                log.warning(
                    "[TunnelPool] connection failed on port %d (%s) — re-establishing tunnel",
                    port, exc,
                )
                self._teardown(key)
                self._establish(key)
                port = self._tunnels[key]["port"]
                return pymysql.connect(
                    host="127.0.0.1",
                    port=port,
                    user=cfg["db"]["user"],
                    password=cfg["db"]["password"],
                    database=cfg["db"]["database"],
                    charset="utf8mb4",
                    connect_timeout=30,
                )

    def close_all(self) -> None:
        """Tear down all open tunnels."""
        for key in list(self._tunnels.keys()):
            self._teardown(key)
        log.info("[TunnelPool] all tunnels closed")

    # ── Internal helpers ──────────────────────────────────────────────────────

    def _establish(self, key: str) -> None:
        cfg        = self._cfg_map[key]
        ssh        = cfg["ssh"]
        client     = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(
            hostname=ssh["host"],
            port=ssh["port"],
            username=ssh["username"],
            key_filename=ssh["pkey_path"],
        )
        transport = client.get_transport()

        server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_sock.bind(("127.0.0.1", 0))
        local_port = server_sock.getsockname()[1]
        server_sock.listen(10)
        server_sock.settimeout(1.0)

        stop = threading.Event()

        def _serve():
            while not stop.is_set():
                try:
                    conn_sock, _ = server_sock.accept()
                except socket.timeout:
                    continue
                except Exception:
                    break
                try:
                    ch = transport.open_channel(
                        "direct-tcpip",
                        (ssh["remote_bind_address"], ssh["remote_bind_port"]),
                        ("127.0.0.1", 0),
                    )
                except Exception as e:
                    log.warning("[TunnelPool] Could not open channel: %s", e)
                    conn_sock.close()
                    continue
                threading.Thread(target=_bridge, args=(conn_sock, ch), daemon=True).start()

        threading.Thread(target=_serve, daemon=True).start()

        self._tunnels[key] = {
            "port":        local_port,
            "client":      client,
            "server_sock": server_sock,
            "stop":        stop,
        }

    def _teardown(self, key: str) -> None:
        t = self._tunnels.pop(key, None)
        if not t:
            return
        t["stop"].set()
        try:
            t["server_sock"].close()
        except Exception:
            pass
        try:
            t["client"].close()
        except Exception:
            pass


def _cfg_key(cfg: Dict[str, Any]) -> str:
    """Stable string key for a DB config dict (used by TunnelPool)."""
    ssh = cfg["ssh"]
    return f"{ssh['host']}:{ssh['port']}:{ssh['remote_bind_address']}:{ssh['remote_bind_port']}"


@contextmanager
def _connect_or_pool(cfg: Dict[str, Any]):
    """
    Use the active TunnelPool if one is open; otherwise fall back to a fresh tunnel.
    Yields a pymysql connection.  Caller must NOT close pooled connections — the
    context manager handles it.
    """
    if DB_DIRECT:
        conn = _direct_connect(cfg)
        try:
            yield conn
        finally:
            conn.close()
        return

    pool = TunnelPool._active
    if pool is not None:
        conn = pool.get_conn(cfg)
        try:
            yield conn
        finally:
            conn.close()
    else:
        with _connect(cfg) as conn:
            yield conn


# ─────────────────────────────────────────────────────────────────────────────
# Public helpers
# ─────────────────────────────────────────────────────────────────────────────

def fetch(cfg: Dict[str, Any], sql: str, params: Optional[Tuple] = None) -> pd.DataFrame:
    """
    Run a SELECT through an SSH tunnel (or pool) and return a DataFrame.

    Args:
        cfg:    Config dict with 'ssh' and 'db' sub-dicts (SOURCE_DB or ANALYTICS_DB).
        sql:    Query string with %s placeholders.
        params: Tuple of parameter values matching %s placeholders.
    """
    with _connect_or_pool(cfg) as conn:
        with conn.cursor() as cur:
            if params is None:
                cur.execute(sql)
            else:
                cur.execute(sql, params)
            columns = [d[0] for d in cur.description]
            rows    = cur.fetchall()
    df = pd.DataFrame(rows, columns=columns)
    log.debug("fetch → %d rows", len(df))
    return df


def delete_user_rows(
    cfg:      Dict[str, Any],
    table:    str,
    user_ids: list,
) -> None:
    """
    Delete all rows for the given user_ids from a table.
    Used by incremental runs to remove stale data before re-inserting.
    Safe to call when the table does not yet exist (no-op in that case).
    """
    if not user_ids:
        return
    ph = ", ".join(["%s"] * len(user_ids))
    with _connect_or_pool(cfg) as conn:
        with conn.cursor() as cur:
            try:
                cur.execute(
                    f"DELETE FROM `{table}` WHERE user_id IN ({ph})",
                    tuple(user_ids),
                )
            except pymysql.err.ProgrammingError as exc:
                if exc.args[0] == 1146:   # table doesn't exist yet — nothing to delete
                    return
                raise
        conn.commit()
    log.debug("delete_user_rows → %s (%d user_ids)", table, len(user_ids))


_DTYPE_TO_MYSQL = {
    "object":         "TEXT",
    "string":         "TEXT",
    "int8":           "TINYINT",
    "int16":          "SMALLINT",
    "int32":          "INT",
    "int64":          "BIGINT",
    "Int8":           "TINYINT",
    "Int16":          "SMALLINT",
    "Int32":          "INT",
    "Int64":          "BIGINT",
    "float32":        "FLOAT",
    "float64":        "DOUBLE",
    "Float32":        "FLOAT",
    "Float64":        "DOUBLE",
    "bool":           "TINYINT(1)",
    "boolean":        "TINYINT(1)",
    "datetime64[ns]": "DATETIME",
}


def _create_table_sql(table: str, df: pd.DataFrame) -> str:
    """Generate CREATE TABLE SQL inferred from a DataFrame's dtypes."""
    col_defs = ", ".join(
        f"`{col}` {_DTYPE_TO_MYSQL.get(str(dtype), 'TEXT')}"
        for col, dtype in df.dtypes.items()
    )
    return f"CREATE TABLE IF NOT EXISTS `{table}` ({col_defs})"


def write_table(
    cfg:       Dict[str, Any],
    df:        pd.DataFrame,
    table:     str,
    if_exists: str = "replace",
) -> None:
    """
    Write a DataFrame to a MySQL table through an SSH tunnel (or pool).

    Args:
        cfg:       Config dict with 'ssh' and 'db' sub-dicts.
        df:        DataFrame to write.
        table:     Target table name.
        if_exists: 'replace' → DROP + CREATE + INSERT (schema always matches df).
                   'append'  → INSERT only (table must already exist).

    Rows are inserted in batches of CHUNK_SIZE (default 5000).
    """
    if df.empty:
        log.warning("write_table called with empty DataFrame — skipping %s", table)
        return

    df = df.astype(object).where(pd.notnull(df), other=None)

    cols         = list(df.columns)
    cols_sql     = ", ".join(f"`{c}`" for c in cols)
    placeholders = ", ".join(["%s"] * len(cols))
    insert_sql   = f"INSERT INTO `{table}` ({cols_sql}) VALUES ({placeholders})"
    rows         = [tuple(row) for row in df.itertuples(index=False, name=None)]

    with _connect_or_pool(cfg) as conn:
        with conn.cursor() as cur:
            if if_exists == "replace":
                cur.execute(f"DROP TABLE IF EXISTS `{table}`")
                cur.execute(_create_table_sql(table, df))
                log.debug("Recreated table %s", table)
            for i in range(0, len(rows), CHUNK_SIZE):
                batch = rows[i : i + CHUNK_SIZE]
                cur.executemany(insert_sql, batch)
                log.debug("Inserted chunk %d–%d → %s", i, i + len(batch), table)
        conn.commit()

    log.info("write_table → %d rows written to %s.%s",
             len(df), cfg["db"]["database"], table)


def write_table_with_conn(
    conn:      pymysql.connections.Connection,
    db_name:   str,
    df:        pd.DataFrame,
    table:     str,
    if_exists: str = "replace",
) -> None:
    """
    Write a DataFrame using a caller-supplied live pymysql connection.

    Used by ResultBuffer.flush() so the entire streaming flush shares one
    SSH tunnel instead of reopening per batch.

    Args:
        conn:      An open pymysql connection (caller manages lifecycle).
        db_name:   The database name — used only for log messages.
        df:        DataFrame to write.
        table:     Target table name.
        if_exists: 'replace' or 'append' (same semantics as write_table).
    """
    if df.empty:
        return

    df = df.astype(object).where(pd.notnull(df), other=None)

    cols         = list(df.columns)
    cols_sql     = ", ".join(f"`{c}`" for c in cols)
    placeholders = ", ".join(["%s"] * len(cols))
    insert_sql   = f"INSERT INTO `{table}` ({cols_sql}) VALUES ({placeholders})"
    rows         = [tuple(row) for row in df.itertuples(index=False, name=None)]

    with conn.cursor() as cur:
        if if_exists == "replace":
            cur.execute(f"DROP TABLE IF EXISTS `{table}`")
            cur.execute(_create_table_sql(table, df))
            log.debug("Recreated table %s", table)
        for i in range(0, len(rows), CHUNK_SIZE):
            batch = rows[i : i + CHUNK_SIZE]
            cur.executemany(insert_sql, batch)
            log.debug("Inserted chunk %d–%d → %s", i, i + len(batch), table)
    conn.commit()
    log.info("write_table_with_conn → %d rows written to %s.%s", len(df), db_name, table)


def run_sql(cfg: Dict[str, Any], statements: List[str]) -> None:
    """Execute one or more SQL statements (DDL/DML) through an SSH tunnel (or pool)."""
    with _connect_or_pool(cfg) as conn:
        with conn.cursor() as cur:
            for sql in statements:
                log.debug("run_sql: %s", sql[:120])
                cur.execute(sql)
        conn.commit()

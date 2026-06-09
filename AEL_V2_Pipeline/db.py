import logging
import select
import socket
import threading
from contextlib import contextmanager
from typing import Any, Dict, Optional, Tuple

import pandas as pd
import paramiko
import pymysql

from config import CHUNK_SIZE

log = logging.getLogger(__name__)


# ── Local port forwarder ──────────────────────────────────────────────────────

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
    # ── 1. Connect to bastion ─────────────────────────────────────────────────
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

    # ── 2. Bind a free local port ─────────────────────────────────────────────
    server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_sock.bind(("127.0.0.1", 0))
    local_port = server_sock.getsockname()[1]
    server_sock.listen(5)
    server_sock.settimeout(1.0)

    # ── 3. Forwarding thread ─────────────────────────────────────────────────
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
    log.debug("Local forwarder listening on 127.0.0.1:%d → %s:%d",
              local_port, ssh["remote_bind_address"], ssh["remote_bind_port"])

    class _Tunnel:
        local_bind_port = local_port

    try:
        yield _Tunnel()
    finally:
        stop.set()
        server_sock.close()
        client.close()
        log.debug("SSH disconnected ← %s", ssh["host"])


# ── MySQL connection through the tunnel ───────────────────────────────────────

@contextmanager
def _connect(cfg: Dict[str, Any]):
    """
    Open an SSH tunnel, then connect pymysql to the forwarded local port.

    cfg shape:
        {
          "ssh": { host, port, username, pkey_path,
                   remote_bind_address, remote_bind_port },
          "db":  { user, password, database }
        }
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


# ── Public helpers ────────────────────────────────────────────────────────────

def fetch(cfg: Dict[str, Any], sql: str, params: Optional[Tuple] = None) -> pd.DataFrame:
    """
    Run a SELECT through an SSH tunnel and return a DataFrame.

    Uses cursor.execute() directly to avoid the pandas SQLAlchemy warning
    when passing a raw pymysql connection.

    Args:
        cfg:    Config dict with 'ssh' and 'db' sub-dicts (SOURCE_DB or ANALYTICS_DB).
        sql:    Query string with %s placeholders.
        params: Tuple of parameter values matching %s placeholders.
    """
    with _connect(cfg) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params or ())
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
    with _connect(cfg) as conn:
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
    cfg: Dict[str, Any],
    df: pd.DataFrame,
    table: str,
    if_exists: str = "replace",
) -> None:
    """
    Write a DataFrame to a MySQL table through an SSH tunnel.

    Args:
        cfg:       Config dict with 'ssh' and 'db' sub-dicts.
        df:        DataFrame to write. Column names must match the target table.
        table:     Target table name.
        if_exists: 'replace' → TRUNCATE then INSERT (keeps existing schema).
                              If the table does not exist yet it is created
                              automatically from the DataFrame dtypes.
                   'append'  → INSERT only (table must already exist).

    Rows are inserted in batches of CHUNK_SIZE (default 5000).
    """
    if df.empty:
        log.warning("write_table called with empty DataFrame — skipping %s", table)
        return

    # Replace NaN / NaT with None so pymysql sends NULL to MySQL.
    # Must cast to object dtype first — float columns otherwise keep numpy nan
    # which pymysql cannot serialise (raises "nan can not be used with MySQL").
    df = df.astype(object).where(pd.notnull(df), other=None)

    cols         = list(df.columns)
    cols_sql     = ", ".join(f"`{c}`" for c in cols)
    placeholders = ", ".join(["%s"] * len(cols))
    insert_sql   = f"INSERT INTO `{table}` ({cols_sql}) VALUES ({placeholders})"
    rows         = [tuple(row) for row in df.itertuples(index=False, name=None)]

    with _connect(cfg) as conn:
        with conn.cursor() as cur:
            if if_exists == "replace":
                # DROP + CREATE ensures the schema always matches the current DataFrame.
                # TRUNCATE would keep the old schema and break when new columns are added.
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


def run_sql(cfg: Dict[str, Any], statements: list[str]) -> None:
    """Execute one or more SQL statements (DDL/DML) through an SSH tunnel."""
    with _connect(cfg) as conn:
        with conn.cursor() as cur:
            for sql in statements:
                log.debug("run_sql: %s", sql[:120])
                cur.execute(sql)
        conn.commit()

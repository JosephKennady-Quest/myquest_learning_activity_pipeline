import argparse
import logging
import re
from pathlib import Path

from config import ANALYTICS_DB, SOURCE_DB
from db import TunnelPool, fetch, write_table


DEFAULT_SQL_PATH = Path("sql_queries/production_user_one_record_subject_project_combo.sql")
DEFAULT_TARGET_TABLE = "production_users_one_record"
DEFAULT_CENTRE_QUERY = """
SELECT c.id
FROM centres c
"""


PARAM_REPLACEMENTS = {
    "user_id": "CAST(NULL AS CHAR(36)) COLLATE utf8mb4_unicode_ci AS user_id",
    "batch_id": "CAST(NULL AS CHAR(36)) COLLATE utf8mb4_unicode_ci AS batch_id",
}

UUID_RE = re.compile(
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
)


def _replace_param(sql: str, param_name: str, replacement_expr: str) -> str:
    pattern = re.compile(
        rf"^\s*.+?\s+COLLATE\s+utf8mb4_unicode_ci\s+AS\s+{param_name}\s*,?\s*$",
        re.MULTILINE,
    )
    suffix = "," if param_name != "phase_id" else ""
    replacement = f"        {replacement_expr}{suffix}"
    updated, count = pattern.subn(replacement, sql, count=1)
    if count != 1:
        raise ValueError(f"Could not replace params.{param_name} in SQL template")
    return updated


def build_sql_for_centre(template_sql: str, centre_id: str) -> str:
    if not UUID_RE.match(centre_id):
        raise ValueError(f"Invalid centre id: {centre_id}")

    sql = template_sql
    sql = _replace_param(sql, "user_id", PARAM_REPLACEMENTS["user_id"])
    sql = _replace_param(
        sql,
        "centre_id",
        f"_utf8mb4'{centre_id}' COLLATE utf8mb4_unicode_ci AS centre_id",
    )
    sql = _replace_param(sql, "batch_id", PARAM_REPLACEMENTS["batch_id"])
    return sql


def get_centre_ids(centre_sql_path: Path | None, limit: int | None) -> list[str]:
    if centre_sql_path is not None:
        sql = centre_sql_path.read_text()
    elif limit is not None:
        sql = f"""
SELECT c.id
FROM centres c
LIMIT {int(limit)}
"""
    else:
        sql = DEFAULT_CENTRE_QUERY

    centres = fetch(SOURCE_DB, sql)
    id_column = "id" if "id" in centres.columns else centres.columns[0]
    return centres[id_column].dropna().astype(str).tolist()


def run(
    sql_path: Path,
    centre_sql_path: Path | None,
    target_table: str,
    limit: int | None,
    replace_target: bool,
) -> None:
    sql_template = sql_path.read_text()
    centre_ids = get_centre_ids(centre_sql_path, limit)

    logging.info("Found %d centre ids", len(centre_ids))
    if not centre_ids:
        logging.warning("No centres found. Nothing to write.")
        return

    target_created = not replace_target

    with TunnelPool() as pool:
        pool.open(SOURCE_DB)
        pool.open(ANALYTICS_DB)

        for index, centre_id in enumerate(centre_ids, start=1):
            logging.info("Running centre %d/%d: %s", index, len(centre_ids), centre_id)
            centre_sql = build_sql_for_centre(sql_template, centre_id)
            result = fetch(SOURCE_DB, centre_sql)

            if result.empty:
                logging.info("Centre %s returned no rows. Skipping write.", centre_id)
                continue

            if_exists = "append" if target_created else "replace"
            write_table(ANALYTICS_DB, result, target_table, if_exists=if_exists)
            target_created = True
            logging.info("Wrote %d rows for centre %s", len(result), centre_id)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run production user one-record SQL once per centre and write all "
            "results into one target table."
        )
    )
    parser.add_argument(
        "--sql-path",
        type=Path,
        default=DEFAULT_SQL_PATH,
        help=f"SQL template path. Default: {DEFAULT_SQL_PATH}",
    )
    parser.add_argument(
        "--target-table",
        default=DEFAULT_TARGET_TABLE,
        help=f"Destination table name. Default: {DEFAULT_TARGET_TABLE}",
    )
    parser.add_argument(
        "--centre-sql-path",
        type=Path,
        default=None,
        help="Optional SQL file that returns centre IDs in the first column.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=10,
        help="Number of centres to process. Use 0 to process all centres.",
    )
    parser.add_argument(
        "--replace-target",
        action="store_true",
        help="Recreate the target table on the first non-empty centre result.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = parse_args()
    run(
        sql_path=args.sql_path,
        centre_sql_path=args.centre_sql_path,
        target_table=args.target_table,
        limit=None if args.limit == 0 else args.limit,
        replace_target=args.replace_target,
    )

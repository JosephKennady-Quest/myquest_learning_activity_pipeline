"""
Create quest_analytics.main_wcc_json.

This pipeline keeps one row per tlo_user_id from main_users and stores repeating
project/phase and subject combinations as JSON text.
"""

import argparse
import logging
import os
from datetime import datetime

import pandas as pd

from config import ANALYTICS_DB, OUTPUT_DIR
from db import write_table, run_sql
from steps.s4_users_project_phase_json import run_users_project_phase_json

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("main_wcc_json_v2")

DEFAULT_TARGET_TABLE = "main_wcc_json_v2"


def parse_args():
    parser = argparse.ArgumentParser(
        description="Build one-row-per-user JSON table from main_users/project/phase/subject tables."
    )
    parser.add_argument("--user-id", default=None, help="Filter to a single tlo_user_id")
    parser.add_argument("--centre-id", default=None, help="Filter to a single centre_id")
    parser.add_argument("--batch-id", default=None, help="Filter to a single batch_id")
    parser.add_argument(
        "--target-table",
        default=DEFAULT_TARGET_TABLE,
        help=f"Target analytics table name (default: {DEFAULT_TARGET_TABLE})",
    )
    parser.add_argument(
        "--output",
        choices=["db", "csv", "both"],
        default="db",
        help="Where to write results (default: db)",
    )
    parser.add_argument("--dry-run", action="store_true", help="Build and validate only")
    parser.add_argument("--print", action="store_true", help="Print first 5 output rows")
    return parser.parse_args()


def _save_csv(df: pd.DataFrame, target_table: str) -> str:
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = os.path.join(OUTPUT_DIR, f"{target_table}_{ts}.csv")
    df.to_csv(path, index=False)
    log.info("CSV saved -> %s", path)
    return path


def main():
    args = parse_args()

    final_df = run_users_project_phase_json(
        user_id=args.user_id,
        centre_id=args.centre_id,
        batch_id=args.batch_id,
    )

    log.info("Final user rows: %d", len(final_df))
    if "project_phase_combos" in final_df.columns:
        filled = final_df["project_phase_combos"].notna().sum()
        log.info("Rows with project_phase_combos: %d", filled)
    if "subject_combos" in final_df.columns:
        filled = final_df["subject_combos"].notna().sum()
        log.info("Rows with subject_combos: %d", filled)

    if args.print:
        pd.set_option("display.max_columns", None)
        pd.set_option("display.max_colwidth", 500)
        print(final_df.head(5).to_string(index=False))

    if args.dry_run:
        log.info("Dry run complete. No output written.")
        return

    if args.output in {"csv", "both"}:
        _save_csv(final_df, args.target_table)

    if args.output in {"db", "both"}:
        write_table(
            ANALYTICS_DB,
            final_df,
            table=args.target_table,
            if_exists="replace",
        )
        log.info(
            "Done. %d rows written to %s.%s",
            len(final_df),
            ANALYTICS_DB["db"]["database"],
            args.target_table,
        )

        t = f"`{args.target_table}`"
        log.info("Optimising table schema and creating index …")
        run_sql(ANALYTICS_DB, [
            f"""ALTER TABLE {t}
                MODIFY `tlo_users_id`         VARCHAR(36),
                MODIFY `user_name`             VARCHAR(250),
                MODIFY `gender`                VARCHAR(12),
                MODIFY `created_at`            DATE,
                MODIFY `centre_name`           VARCHAR(100),
                MODIFY `org_name`              VARCHAR(70),
                MODIFY `state_name`            VARCHAR(25),
                MODIFY `district_name`         VARCHAR(35),
                MODIFY `trade`                 VARCHAR(60),
                MODIFY `batch_name`            VARCHAR(90),
                MODIFY `batch_status`          VARCHAR(12),
                MODIFY `centre_type`           VARCHAR(35),
                MODIFY `user_type`             VARCHAR(15),
                MODIFY `platform`              VARCHAR(20),
                MODIFY `is_ple`                VARCHAR(1),
                MODIFY `ple_enabled`           VARCHAR(15),
                MODIFY `project_phase_combos`  TEXT,
                MODIFY `subject_combos`        LONGTEXT,
                MODIFY `a_overa_less_asses_c`  INT,
                MODIFY `a_overa_assess_c`      INT,
                MODIFY `a_overa_lesson_c`      INT,
                MODIFY `c_overa_less_asses_c`  INT,
                MODIFY `c_overa_asse_c`        INT,
                MODIFY `c_overa_less_c`        INT,
                MODIFY `rounded_completion`    DECIMAL(10,2),
                MODIFY `first_login`           DATE""",
            f"CREATE INDEX `idx_tlo_users_id` ON {t} (`tlo_users_id`)",
        ])
        log.info("Schema optimisation and index creation complete.")


if __name__ == "__main__":
    main()

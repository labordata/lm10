"""Merge staging CSVs (a partial scrape) into an existing lm10.db.

Each CSV in `staging_dir` is loaded into a `temp.staging_<name>` table on
the lm10.db connection, then copied into the real tables with scoped
DELETE+INSERT. The activity table's `id` PK is auto-assigned past the
existing max; activity-child rows (counterparty_contact,
counterparty_organization, expenditure) have their per-rptId
`activity_id` ordinals resolved to the new global ids via a join. All
staging tables vanish when the connection closes.

Re-running with the same staging dir is idempotent.
"""

import argparse
import csv
import logging
import sqlite3
import sys
from pathlib import Path

import dateparser

logger = logging.getLogger("merge_incremental")

# CSVs we know how to merge. Names match destination tables.
RPT_KEYED_TABLES = (
    "filing", "lm10", "organization",
    "signature", "other_address", "principal_officer",
    "reportable_activity", "reporting_employer",
)
ACTIVITY_CHILDREN = (
    "counterparty_contact", "counterparty_organization", "expenditure",
)
ALL_CSVS = (
    *RPT_KEYED_TABLES, "activity", *ACTIVITY_CHILDREN, "filer",
)

# Date columns the full build re-parses via sqlite-utils' r.parsedate.
DATE_COLUMNS = {
    "lm10": ["period_begin", "period_through"],
    "expenditure": ["date"],
    "signature": ["on_date"],
    "activity": ["date_of_agreement"],
    "organization": ["promiseDate"],
}

NONE_LIKE = {"", "none", "not available"}


def parsedate(v):
    if v is None:
        return None
    s = str(v).strip()
    if s.lower() in NONE_LIKE:
        return None
    d = dateparser.parse(s)
    return d.date().isoformat() if d else None


def quote_cols(cols):
    return ", ".join(f'"{c}"' for c in cols)


def load_csv_to_temp(conn, csv_path, temp_table):
    with open(csv_path, newline="") as f:
        reader = csv.reader(f)
        try:
            header = next(reader)
        except StopIteration:
            return None
        conn.execute(
            f'CREATE TEMP TABLE "{temp_table}" ({quote_cols(header)})'
        )
        placeholders = ", ".join("?" * len(header))
        conn.executemany(
            f'INSERT INTO "{temp_table}" VALUES ({placeholders})',
            (row for row in reader if any(cell != "" for cell in row)),
        )
    return header


def table_columns(conn, table, schema="main"):
    return [r[1] for r in conn.execute(f'PRAGMA {schema}.table_info("{table}")')]


def temp_table_names(conn):
    return {
        r[0] for r in conn.execute(
            "SELECT name FROM sqlite_temp_master WHERE type='table'"
        )
    }


def merge_rpt_keyed(conn, name):
    src = f"staging_{name}"
    dest_cols = table_columns(conn, name)
    src_cols = table_columns(conn, src, schema="temp")
    common = [c for c in dest_cols if c in src_cols]
    if "rptId" not in src_cols:
        logger.warning("%s: staging table has no rptId; skipping", name)
        return
    if not common:
        logger.warning("%s: no column overlap; skipping", name)
        return
    conn.execute(
        f'DELETE FROM main."{name}" WHERE rptId IN '
        f'(SELECT DISTINCT CAST(rptId AS INTEGER) FROM temp."{src}")'
    )
    conn.execute(
        f'INSERT INTO main."{name}" ({quote_cols(common)}) '
        f'SELECT {quote_cols(common)} FROM temp."{src}"'
    )
    n = conn.execute(f'SELECT COUNT(*) FROM temp."{src}"').fetchone()[0]
    logger.info("%s: merged %d rows", name, n)


def merge_activity_and_children(conn):
    present = temp_table_names(conn)
    if "staging_activity" not in present:
        return

    new_rpts = "SELECT DISTINCT CAST(rptId AS INTEGER) FROM temp.staging_activity"

    # Delete existing activity-child rows for the new rptIds, then the activity
    # rows themselves. (Order matters: children FK to activity.id.)
    old_act_ids = f"SELECT id FROM main.activity WHERE rptId IN ({new_rpts})"
    for child in ACTIVITY_CHILDREN:
        if f"staging_{child}" in present:
            conn.execute(
                f'DELETE FROM main."{child}" WHERE activity_id IN ({old_act_ids})'
            )
    conn.execute(f"DELETE FROM main.activity WHERE rptId IN ({new_rpts})")

    # Insert activity rows in deterministic order so we can recover the
    # (rptId, ordinal) → global id mapping with ROW_NUMBER().
    dest_cols = [c for c in table_columns(conn, "activity") if c != "id"]
    src_cols = table_columns(conn, "staging_activity", schema="temp")
    common = [c for c in dest_cols if c in src_cols]
    conn.execute(
        f'INSERT INTO main.activity ({quote_cols(common)}) '
        f'SELECT {quote_cols(common)} FROM temp.staging_activity '
        f'ORDER BY CAST(rptId AS INTEGER), CAST(activity_id AS INTEGER)'
    )
    n_act = conn.execute(
        "SELECT COUNT(*) FROM temp.staging_activity"
    ).fetchone()[0]
    logger.info("activity: merged %d rows", n_act)

    conn.execute(
        f'CREATE TEMP TABLE activity_id_map AS '
        f'SELECT rptId, ROW_NUMBER() OVER (PARTITION BY rptId ORDER BY id) - 1 '
        f'    AS ordinal, id '
        f'FROM main.activity '
        f'WHERE rptId IN ({new_rpts})'
    )

    for child in ACTIVITY_CHILDREN:
        src = f"staging_{child}"
        if src not in present:
            continue
        dest_cols = table_columns(conn, child)
        src_cols = table_columns(conn, src, schema="temp")
        passthrough = [c for c in dest_cols if c in src_cols and c != "activity_id"]
        select_exprs = ["m.id"] + [f's."{c}"' for c in passthrough]
        insert_cols = ["activity_id"] + passthrough
        conn.execute(
            f'INSERT INTO main."{child}" ({quote_cols(insert_cols)}) '
            f'SELECT {", ".join(select_exprs)} '
            f'FROM temp."{src}" s '
            f'JOIN temp.activity_id_map m '
            f'  ON m.rptId = CAST(s.rptId AS INTEGER) '
            f' AND m.ordinal = CAST(s.activity_id AS INTEGER)'
        )
        n = conn.execute(f'SELECT COUNT(*) FROM temp."{src}"').fetchone()[0]
        logger.info("%s: merged %d rows", child, n)


def merge_filer(conn):
    if "staging_filer" not in temp_table_names(conn):
        return
    dest_cols = table_columns(conn, "filer")
    src_cols = table_columns(conn, "staging_filer", schema="temp")
    common = [c for c in dest_cols if c in src_cols]
    conn.execute(
        f'INSERT OR REPLACE INTO main.filer ({quote_cols(common)}) '
        f'SELECT {quote_cols(common)} FROM temp.staging_filer'
    )
    n = conn.execute("SELECT COUNT(*) FROM temp.staging_filer").fetchone()[0]
    logger.info("filer: upserted %d rows", n)


def reparse_dates_for_new_rpts(conn, new_rpt_ids):
    if not new_rpt_ids:
        return
    placeholders = ",".join("?" * len(new_rpt_ids))
    for table, cols in DATE_COLUMNS.items():
        existing = table_columns(conn, table)
        if "rptId" not in existing:
            continue
        for col in cols:
            if col not in existing:
                continue
            rows = conn.execute(
                f'SELECT rowid, "{col}" FROM main."{table}" '
                f'WHERE "rptId" IN ({placeholders})',
                new_rpt_ids,
            ).fetchall()
            updates = [
                (parsedate(val), rowid) for rowid, val in rows
                if parsedate(val) != val
            ]
            if updates:
                conn.executemany(
                    f'UPDATE main."{table}" SET "{col}" = ? WHERE rowid = ?',
                    updates,
                )


def cleanup_null_rows(conn):
    """Drop rows from contact-like tables whose key fields are all NULL."""
    for t in ("counterparty_contact", "principal_officer", "other_address"):
        conn.execute(
            f'DELETE FROM main."{t}" WHERE city IS NULL AND name IS NULL '
            f'AND "po_box,_bldg,_room_no,_if_any" IS NULL '
            f'AND state IS NULL AND street IS NULL AND "zip_code_+_4" IS NULL'
        )


def main():
    logging.basicConfig(level=logging.INFO, format="merge: %(message)s")
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("db", help="lm10.db (modified in place)")
    ap.add_argument("staging_dir", help="directory of staging CSVs")
    args = ap.parse_args()

    staging = Path(args.staging_dir)
    if not staging.is_dir():
        print(f"no staging dir: {staging}", file=sys.stderr)
        sys.exit(1)

    conn = sqlite3.connect(args.db)
    conn.execute("BEGIN")
    try:
        for name in ALL_CSVS:
            p = staging / f"{name}.csv"
            if p.exists():
                load_csv_to_temp(conn, p, f"staging_{name}")

        present = temp_table_names(conn)
        for name in RPT_KEYED_TABLES:
            if f"staging_{name}" in present:
                merge_rpt_keyed(conn, name)

        merge_activity_and_children(conn)
        merge_filer(conn)

        new_rpt_ids = []
        if "staging_filing" in present:
            new_rpt_ids = [
                r[0] for r in conn.execute(
                    "SELECT DISTINCT CAST(rptId AS INTEGER) FROM temp.staging_filing"
                )
            ]
        reparse_dates_for_new_rpts(conn, new_rpt_ids)
        cleanup_null_rows(conn)

        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()

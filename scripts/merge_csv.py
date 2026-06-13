"""Merge a CSV of freshly scraped rows (stdin) into a table of lm10.db.

Rows for any rptId in the incoming batch replace that rptId's existing
rows. CSV columns are matched to table columns BY NAME, so the
batch-dependent column subsets and orderings that json-to-multicsv
emits are safe: columns missing from a batch are stored as NULL, empty
strings become NULL (matching the csvs-to-sqlite behavior of the full
build), and unknown columns are an error unless --ignore'd.

Table-specific behavior, mirroring the full build in the Makefile:

- filer: full-crawl upsert (REPLACE INTO on the srNum primary key);
  rows are never deleted, so filers absent from one crawl are kept.
- filing: upsert on the rptId primary key (LM-10 amendments reuse the
  same rptId). Previously archived file_path/file_checksum/file_status
  are preserved when the incoming row's download failed.
- lm10: one row per successfully parsed form, so its incoming rptIds
  are the set whose form-derived children (signature, other_address,
  principal_officer, reportable_activity, reporting_employer, activity
  and its children) can safely be cleared and rebuilt. Keying the
  clearing on the form batch — NOT the filing batch — matters because
  the spider also yields filings whose form parse failed.
- activity: the id primary key is reassigned on insert, so child rows
  are cascade-deleted first and rows are inserted in (rptId, ordinal)
  order so children can re-resolve their ordinals.
- counterparty_contact / counterparty_organization / expenditure: the
  CSV's activity_id column holds the per-rptId ordinal; it is resolved
  to activity.id by insertion order, like the full build's
  sqlite-utils update.
- organization: rows come from a separate crawl; rows whose rptId is
  not in filing are skipped (the filings crawl can drop a filing the
  organizations crawl still reports on).

Usage: python scripts/merge_csv.py lm10.db TABLE [--replace]
       [--ignore COLUMN]... < table.csv
"""

import argparse
import csv
import sqlite3
import sys

# Tables whose rows derive from a successfully parsed form. Cleared for
# the lm10 batch's rptIds and repopulated by their own merges.
FORM_CHILD_TABLES = (
    "signature",
    "other_address",
    "principal_officer",
    "reportable_activity",
    "reporting_employer",
)

ACTIVITY_CHILD_TABLES = (
    "counterparty_contact",
    "counterparty_organization",
    "expenditure",
)

# The full build deletes rows where every one of these columns is
# null (empty contact blocks the form parser emits for blank sections).
EMPTY_ROW_CHECK = {
    "counterparty_contact": (
        "city",
        "name",
        "po_box,_bldg,_room_no,_if_any",
        "state",
        "street",
        "zip_code_+_4",
    ),
    "principal_officer": (
        "city",
        "name",
        "po_box,_bldg,_room_no,_if_any",
        "state",
        "street",
        "zip_code_+_4",
    ),
    "other_address": (
        "city",
        "name",
        "po_box,_bldg,_room_no,_if_any",
        "state",
        "street",
        "zip_code_+_4",
    ),
}

FILE_POINTER_COLUMNS = ("file_path", "file_checksum", "file_status")


def quoted(column):
    return '"' + column.replace('"', '""') + '"'


def table_columns(conn, table):
    return [row[1] for row in conn.execute(f"PRAGMA table_info({quoted(table)})")]


def delete_by_rptid(conn, table, rpt_ids):
    if not rpt_ids:
        return 0
    placeholders = ", ".join("?" * len(rpt_ids))
    cursor = conn.execute(
        f"DELETE FROM {quoted(table)} WHERE rptId IN ({placeholders})",
        sorted(rpt_ids),
    )
    return cursor.rowcount


def incoming_rptids(rows):
    return {int(row["rptId"]) for row in rows}


def insert_rows(conn, table, columns, rows, replace=False):
    verb = "REPLACE" if replace else "INSERT"
    column_list = ", ".join(quoted(column) for column in columns)
    placeholders = ", ".join("?" * len(columns))
    conn.executemany(
        f"{verb} INTO {quoted(table)} ({column_list}) VALUES ({placeholders})",
        [tuple(row[column] for column in columns) for row in rows],
    )
    return len(rows)


def clear_activities(conn, rpt_ids):
    """Delete activity rows for `rpt_ids` and, first, the child rows
    that reference their soon-to-be-reused ids."""
    if not rpt_ids:
        return
    placeholders = ", ".join("?" * len(rpt_ids))
    for child in ACTIVITY_CHILD_TABLES:
        conn.execute(
            f"DELETE FROM {quoted(child)} WHERE activity_id IN"
            f" (SELECT id FROM activity WHERE rptId IN ({placeholders}))",
            sorted(rpt_ids),
        )
    delete_by_rptid(conn, "activity", rpt_ids)


def merge_filing(conn, columns, rows):
    # keep previously archived file pointers when this crawl's download
    # failed (the row would otherwise REPLACE them with NULL)
    if all(column in columns for column in FILE_POINTER_COLUMNS):
        for row in rows:
            if row["file_path"] is not None:
                continue
            existing = conn.execute(
                "SELECT file_path, file_checksum, file_status"
                " FROM filing WHERE rptId = ? AND file_path IS NOT NULL",
                (int(row["rptId"]),),
            ).fetchone()
            if existing:
                row.update(zip(FILE_POINTER_COLUMNS, existing))
    return 0, insert_rows(conn, "filing", columns, rows, replace=True)


def merge_lm10(conn, columns, rows):
    # every incoming row is a successfully parsed form, so this batch's
    # rptIds are exactly the set whose form-derived children are about
    # to be re-supplied; clear them here so children that shrank or
    # vanished don't leave stale rows
    rpt_ids = incoming_rptids(rows)
    clear_activities(conn, rpt_ids)
    for child in FORM_CHILD_TABLES:
        delete_by_rptid(conn, child, rpt_ids)
    deleted = delete_by_rptid(conn, "lm10", rpt_ids)
    return deleted, insert_rows(conn, "lm10", columns, rows)


def merge_activity(conn, columns, rows):
    rpt_ids = incoming_rptids(rows)
    clear_activities(conn, rpt_ids)
    # insert in (rptId, ordinal) order so the reassigned ids are
    # monotonic and children can re-resolve their ordinals
    ordered = sorted(rows, key=lambda r: (int(r["rptId"]), int(r["activity_id"])))
    insert_columns = [column for column in columns if column != "activity_id"]
    return 0, insert_rows(conn, "activity", insert_columns, ordered)


def merge_activity_child(conn, table, columns, rows):
    rpt_ids = incoming_rptids(rows)
    placeholders = ", ".join("?" * len(rpt_ids))
    ordinals = {}
    activity_ids = {}
    for activity_id, rpt_id in conn.execute(
        "SELECT id, rptId FROM activity"
        f" WHERE rptId IN ({placeholders}) ORDER BY rptId, id",
        sorted(rpt_ids),
    ):
        activity_ids[(rpt_id, ordinals.get(rpt_id, 0))] = activity_id
        ordinals[rpt_id] = ordinals.get(rpt_id, 0) + 1

    resolved = []
    for row in rows:
        key = (int(row["rptId"]), int(row["activity_id"]))
        if key not in activity_ids:
            print(
                f"{table}: no activity for rptId {key[0]}, ordinal {key[1]};"
                " skipping row",
                file=sys.stderr,
            )
            continue
        resolved.append({**row, "activity_id": activity_ids[key]})

    insert_columns = [column for column in columns if column != "rptId"]
    return 0, insert_rows(conn, table, insert_columns, resolved)


def merge_organization(conn, columns, rows):
    deleted = delete_by_rptid(conn, "organization", incoming_rptids(rows))
    known = {
        rpt_id for (rpt_id,) in conn.execute("SELECT rptId FROM filing")
    }
    # The filings crawl occasionally drops a filing the organizations
    # crawl still reports on; skip those rows instead of violating the
    # FK — the next crawl of that filer (or the full rebuild) heals it.
    insertable = [row for row in rows if int(row["rptId"]) in known]
    if len(insertable) < len(rows):
        print(
            f"organization: skipped {len(rows) - len(insertable)} rows"
            " whose rptId is not in filing",
            file=sys.stderr,
        )
    return deleted, insert_rows(conn, "organization", columns, insertable)


def main():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("db")
    parser.add_argument("table")
    parser.add_argument(
        "--replace",
        action="store_true",
        help="upsert on the primary key instead of deleting by rptId",
    )
    parser.add_argument(
        "--ignore",
        action="append",
        default=[],
        metavar="COLUMN",
        help="CSV column to drop (a field the full build's sqlite-utils"
        " transform drops)",
    )
    args = parser.parse_args()

    reader = csv.DictReader(sys.stdin)
    raw_rows = list(reader)
    if not raw_rows or not reader.fieldnames:
        print(f"{args.table}: no rows to merge", file=sys.stderr)
        return

    header = [column for column in reader.fieldnames if column not in args.ignore]
    if len(set(header)) != len(header):
        raise SystemExit(f"{args.table}: duplicate CSV columns: {header}")

    conn = sqlite3.connect(args.db)
    columns = table_columns(conn, args.table)
    if not columns:
        raise SystemExit(f"{args.db} has no table {args.table}")
    resolved_internally = {"rptId", "activity_id"} if args.table in ACTIVITY_CHILD_TABLES else set()
    if args.table == "activity":
        resolved_internally = {"activity_id"}
    unknown = sorted(set(header) - set(columns) - resolved_internally)
    if unknown:
        raise SystemExit(
            f"{args.table}: CSV columns {unknown} are not in the table;"
            " if the full build drops them, pass --ignore"
        )

    rows = [{column: row[column] or None for column in header} for row in raw_rows]

    if args.table in EMPTY_ROW_CHECK:
        check = EMPTY_ROW_CHECK[args.table]
        kept = [row for row in rows if any(row.get(column) for column in check)]
        if len(kept) < len(rows):
            print(
                f"{args.table}: skipped {len(rows) - len(kept)} empty rows",
                file=sys.stderr,
            )
        rows = kept

    with conn:
        if args.table == "filing":
            deleted, inserted = merge_filing(conn, header, rows)
        elif args.table == "lm10":
            deleted, inserted = merge_lm10(conn, header, rows)
        elif args.table == "activity":
            deleted, inserted = merge_activity(conn, header, rows)
        elif args.table in ACTIVITY_CHILD_TABLES:
            deleted, inserted = merge_activity_child(conn, args.table, header, rows)
        elif args.table == "organization":
            deleted, inserted = merge_organization(conn, header, rows)
        elif args.replace:
            deleted, inserted = 0, insert_rows(
                conn, args.table, header, rows, replace=True
            )
        else:
            deleted = delete_by_rptid(conn, args.table, incoming_rptids(rows))
            inserted = insert_rows(conn, args.table, header, rows)
    conn.close()

    print(f"{args.table}: -{deleted} +{inserted} rows", file=sys.stderr)


if __name__ == "__main__":
    main()

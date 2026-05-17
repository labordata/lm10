"""Reconstruct filing.jl from an existing lm10.db.

The Makefile's incremental path treats filing.jl as the cumulative
source of truth and rebuilds lm10.db from it. To bootstrap that pipeline
when only an old lm10.db is available, we serialize the DB back into
the JSON-lines shape the spider would have emitted: one record per
filing, with `detailed_form_data` filled in by joining lm10, activity,
expenditure, counterparty_*, and the various per-filing tables.

Usage:
    python tools/reconstruct_filing_jl.py lm10.db > filing.jl
"""

import argparse
import json
import sqlite3
import sys


FILING_COLS = [
    "amended", "beginDate", "endDate", "registerDate", "formFiled",
    "receiveDate", "promiseDate", "oID", "rptId", "srFilerId", "yrCovered",
    "empLabOrg", "amendment", "empTrdName", "subLabOrg1", "subLabOrg2",
    "formLink", "srNum", "repOrgsCnt", "paperOrElect", "city", "state",
    "address1", "address2", "zip",
]


def fetch_rows(conn, sql, params=()):
    cur = conn.execute(sql, params)
    cols = [c[0] for c in cur.description]
    return [dict(zip(cols, row)) for row in cur]


def reconstruct(db_path):
    conn = sqlite3.connect(db_path)

    # Group helper tables by rptId once, in memory. The DB is tens of MB
    # and dicts here cap out at a few hundred MB; cheaper than per-row
    # queries.
    def by_rpt(table, drop=()):
        rows = fetch_rows(conn, f"SELECT * FROM {table}")
        out = {}
        for r in rows:
            rpt = r.pop("rptId")
            for d in drop:
                r.pop(d, None)
            out.setdefault(rpt, []).append(r)
        return out

    reporting_employer = by_rpt("reporting_employer")
    principal_officer = by_rpt("principal_officer")
    other_address = by_rpt("other_address")
    reportable_activity = by_rpt("reportable_activity")
    signature = by_rpt("signature")
    lm10_per_rpt = {r["rptId"]: r for r in fetch_rows(conn, "SELECT * FROM lm10")}

    # activity is keyed by `id`; expenditure / counterparty_* point at
    # activity.id. Group activities by rptId, and for each activity hang
    # its children off it.
    activities = fetch_rows(conn, "SELECT * FROM activity")
    expenditures = fetch_rows(conn, "SELECT * FROM expenditure")
    cp_contacts = fetch_rows(conn, "SELECT * FROM counterparty_contact")
    cp_orgs = fetch_rows(conn, "SELECT * FROM counterparty_organization")

    exp_by_act = {}
    for e in expenditures:
        exp_by_act.setdefault(e["activity_id"], []).append(
            {k: v for k, v in e.items() if k != "activity_id"}
        )
    cpc_by_act = {}
    for c in cp_contacts:
        cpc_by_act.setdefault(c["activity_id"], []).append(
            {k: v for k, v in c.items() if k != "activity_id"}
        )
    cpo_by_act = {}
    for c in cp_orgs:
        cpo_by_act.setdefault(c["activity_id"], []).append(
            {k: v for k, v in c.items() if k != "activity_id"}
        )

    activities_by_rpt = {}
    for a in activities:
        rpt = a.pop("rptId")
        aid = a.pop("id")
        a["counterparty_contact"] = cpc_by_act.get(aid, [])
        a["counterparty_organization"] = cpo_by_act.get(aid, [])
        a["expenditures"] = exp_by_act.get(aid, [])
        activities_by_rpt.setdefault(rpt, []).append(a)

    # Now emit one filing.jl line per filing row.
    filings = fetch_rows(conn, "SELECT * FROM filing")
    for f in filings:
        rpt = f["rptId"]
        out = {c: f.get(c) for c in FILING_COLS}
        out["file_urls"] = []
        out["file_headers"] = {}
        out["files"] = []
        # filing.jl had `files = [{path, checksum, status, url}]`; the
        # Makefile flattens this to file_path / file_checksum / file_status
        # before building the DB, so include both shapes for safety.
        if f.get("file_path"):
            out["files"] = [{
                "path": f.get("file_path"),
                "checksum": f.get("file_checksum"),
                "status": f.get("file_status"),
                "url": f.get("filing_url"),
            }]
        out["filing_url"] = f.get("filing_url")
        out["file_path"] = f.get("file_path")
        out["file_checksum"] = f.get("file_checksum")
        out["file_status"] = f.get("file_status")

        lm = lm10_per_rpt.get(rpt)
        if lm:
            out["detailed_form_data"] = {
                "file_number": str(lm.get("file_number") or ""),
                "period_begin": lm.get("period_begin"),
                "period_through": lm.get("period_through"),
                "type_of_organization": lm.get("type_of_organization"),
                "where_records": {
                    k: lm.get(k) for k in (
                        "records_hosted_with_reporting_employer",
                        "records_hosted_with_principal_officer",
                        "records_hosted_at_other_address",
                    ) if k in lm
                },
                "reporting_employer": reporting_employer.get(rpt, []),
                "principal_officer": principal_officer.get(rpt, []),
                "other_address": other_address.get(rpt, []),
                "reportable_activity": reportable_activity.get(rpt, []),
                "signatures": _signatures_dict(signature.get(rpt, [])),
                "activity_details": activities_by_rpt.get(rpt, []),
            }
        else:
            out["detailed_form_data"] = None

        sys.stdout.write(json.dumps(out, default=str) + "\n")


def _signatures_dict(rows):
    # The original parse had {13: {...}, 14: {...}}. After the json round
    # trip, keys are strings; downstream is fine with either.
    by_num = {}
    for i, r in enumerate(rows):
        by_num[str(13 + i)] = {k: v for k, v in r.items()}
    return by_num


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("db", help="Path to lm10.db")
    args = ap.parse_args()
    reconstruct(args.db)


if __name__ == "__main__":
    main()

"""Dump organization table from lm10.db back to organization.csv shape.

This complements reconstruct_filing_jl.py for bootstrapping incremental
runs from a DB-only release.

Usage:
    python tools/dump_organization_csv.py lm10.db > organization.csv
"""

import argparse
import csv
import sqlite3
import sys


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("db")
    args = ap.parse_args()

    conn = sqlite3.connect(args.db)
    cur = conn.execute("SELECT * FROM organization")
    cols = [c[0] for c in cur.description]
    w = csv.writer(sys.stdout)
    w.writerow(cols)
    for row in cur:
        w.writerow(row)


if __name__ == "__main__":
    main()

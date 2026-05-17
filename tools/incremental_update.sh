#!/usr/bin/env bash
# Incremental update: discover filers (srNums) with new LM-10 activity
# since the last cumulative `filing.jl`, scrape just those filers, append
# the new data, then rerun the existing Makefile.
#
# The slow detail endpoint (~7s/req) is still called once per filer with
# new activity, but typical daily growth is on the order of 10-30
# filers, so this finishes in minutes. HTTPCACHE shares the detail
# response between the two spider runs.
#
# Usage:
#   tools/incremental_update.sh
#
# Expects cumulative inputs in the working directory:
#   filing.jl, organization.csv
# If either is missing but lm10.db is present, they're reconstructed
# from the DB. If nothing is available, exits 2 so the caller falls
# back to a full crawl.

set -euo pipefail

cd "$(dirname "$0")/.."

PYTHON=${PYTHON:-python}
SCRAPY=${SCRAPY:-scrapy}

# Bootstrap from lm10.db if cumulative inputs are missing.
if [[ ! -s filing.jl && -s lm10.db ]]; then
    echo "incremental_update: filing.jl missing; reconstructing from lm10.db"
    $PYTHON tools/reconstruct_filing_jl.py lm10.db > filing.jl
fi
if [[ ! -s organization.csv && -s lm10.db ]]; then
    echo "incremental_update: organization.csv missing; reconstructing from lm10.db"
    $PYTHON tools/dump_organization_csv.py lm10.db > organization.csv
fi

if [[ ! -s filing.jl || ! -s organization.csv ]]; then
    echo "incremental_update: no cumulative inputs and no DB to bootstrap from;"
    echo "  fall back to a full crawl via 'make lm10.db'." >&2
    exit 2
fi

# 1. Establish the high-water mark for rptIds we've already processed.
MAX_KNOWN=$($PYTHON -c '
import json
m = 0
for line in open("filing.jl"):
    r = json.loads(line).get("rptId", 0)
    if r > m: m = r
print(m)
')
echo "incremental_update: max known rptId = $MAX_KNOWN"

# 2. Discover srNums for filers with new activity.
$PYTHON tools/discover_new_filings.py --max-known "$MAX_KNOWN" > sr_nums.txt
N=$(grep -c . sr_nums.txt || true)
echo "incremental_update: $N filers have new activity"

# Refresh filer.csv unconditionally — the list endpoint is cheap and
# filer metadata can change without producing a new filing. Use a
# temp file so a transient DOL outage doesn't wipe the cumulative
# artifact.
if $SCRAPY crawl filers -L WARNING -O new_filer.csv && [[ -s new_filer.csv ]]; then
    mv new_filer.csv filer.csv
else
    echo "incremental_update: filers spider failed; keeping previous filer.csv" >&2
    rm -f new_filer.csv
fi

if [[ "$N" -eq 0 ]]; then
    echo "incremental_update: no new filings; rebuilding from existing source files."
else
    # 3. Scrape only the affected filers. Both spiders POST the same
    # srNums to the slow GetLM10FilerDetailServlet (~7s/req); HTTPCACHE
    # serves the second pass from memory.
    rm -rf .scrapy/httpcache
    CACHE_SETTINGS=(
        -s HTTPCACHE_ENABLED=True
        -s HTTPCACHE_EXPIRATION_SECS=3600
        -s HTTPCACHE_DIR=.scrapy/httpcache
    )

    $SCRAPY crawl filings_incremental -L WARNING "${CACHE_SETTINGS[@]}" \
        -a sr_nums_file=sr_nums.txt \
        -a max_known_rpt_id="$MAX_KNOWN" \
        -O new_filing.jl

    if [[ -s new_filing.jl ]]; then
        cat new_filing.jl >> filing.jl
    fi
    rm -f new_filing.jl

    $SCRAPY crawl organizations_incremental -L WARNING "${CACHE_SETTINGS[@]}" \
        -a sr_nums_file=sr_nums.txt \
        -a max_known_rpt_id="$MAX_KNOWN" \
        -O new_organization.csv

    if [[ -s new_organization.csv ]]; then
        tail -n +2 new_organization.csv >> organization.csv
    fi
    rm -f new_organization.csv

    rm -rf .scrapy/httpcache
fi

# 4. Force a Makefile rebuild from the updated source files.
rm -f form.json form.csv form.activity*.csv form.other_address.csv \
      form.principal_officer.csv form.reportable_activity.csv \
      form.reporting_employer.csv form.signature.csv \
      filing.json raw_filing.csv filing.csv lm10.csv \
      counterparty_contact.csv counterparty_organization.csv \
      activity.csv expenditure.csv other_address.csv \
      principal_officer.csv reportable_activity.csv \
      reporting_employer.csv signature.csv lm10.db

make lm10.db
echo "incremental_update: done."

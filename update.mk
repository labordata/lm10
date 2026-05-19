# update.mk — incremental update of lm10.db.
#
# Each table has a `scripts/<table>.sql` script that imports its CSV
# into a temp table and merges into the real table. `update_<table>`
# phony targets express the FK order. Filer refresh and discovery are
# always-fresh (depend on FORCE). Intermediate files land in the
# working dir (all gitignored).
#
# Usage: make -f update.mk

PRIOR_DB_URL ?= https://github.com/labordata/lm10/releases/download/nightly/lm10.db.zip

.DELETE_ON_ERROR:

FORM_CSVS := form.csv form.activity.csv \
    form.activity.counterparty_contact.csv \
    form.activity.counterparty_organization.csv \
    form.activity.expenditure.csv \
    form.other_address.csv form.principal_officer.csv \
    form.reportable_activity.csv form.reporting_employer.csv \
    form.signature.csv

.PHONY: update polish_db fk-check FORCE \
        update_filer update_filing update_lm10 update_organization \
        update_signature update_other_address update_principal_officer \
        update_reportable_activity update_reporting_employer \
        update_activity update_counterparty_contact \
        update_counterparty_organization update_expenditure

# ============================================================================
# Entry
# ============================================================================

# Build the always-fresh inputs first; if sr_nums is non-empty,
# recursively run the full per-table cascade. Filer always merges.
update: lm10.db update_filer sr_nums.txt
	@if [ -s sr_nums.txt ]; then \
	    $(MAKE) -f update.mk update_lm10 update_organization \
	        update_signature update_other_address update_principal_officer \
	        update_reportable_activity update_reporting_employer \
	        update_counterparty_contact update_counterparty_organization \
	        update_expenditure; \
	    $(MAKE) -f update.mk polish_db; \
	fi
	@$(MAKE) -f update.mk fk-check

# ============================================================================
# Validation helpers
# ============================================================================

fk-check:
	@violations=$$(sqlite3 lm10.db "PRAGMA foreign_key_check;"); \
	if [ -n "$$violations" ]; then \
	    echo "fk-check: violations in lm10.db:" >&2; \
	    echo "$$violations" >&2; exit 1; \
	fi
	@echo "fk-check: lm10.db has no FK violations"

# Date columns: re-parse via sqlite-utils. Idempotent on already-ISO
# values, so safe to run unscoped after each merge.
polish_db:
	sqlite-utils convert lm10.db lm10 period_begin 'r.parsedate(value)'
	sqlite-utils convert lm10.db lm10 period_through 'r.parsedate(value)'
	sqlite-utils convert lm10.db expenditure date 'r.parsedate(value)'
	sqlite-utils convert lm10.db signature on_date 'r.parsedate(value)'
	sqlite-utils convert lm10.db activity date_of_agreement \
	    'r.parsedate(value) if value.lower() != "none" else None'
	sqlite-utils convert lm10.db organization promiseDate \
	    'r.parsedate(value) if value.lower() not in {"not available", "none"} else None'

# ============================================================================
# Per-table merges (topological, leaves before roots)
# ============================================================================

update_counterparty_contact: counterparty_contact.csv update_activity
	cat $< | sqlite3 lm10.db -init scripts/counterparty_contact.sql -bail

update_counterparty_organization: counterparty_organization.csv update_activity
	cat $< | sqlite3 lm10.db -init scripts/counterparty_organization.sql -bail

update_expenditure: expenditure.csv update_activity
	cat $< | sqlite3 lm10.db -init scripts/expenditure.sql -bail

update_activity: activity.csv update_filing
	cat $< | sqlite3 lm10.db -init scripts/activity.sql -bail

update_lm10: lm10.csv update_filing
	cat $< | sqlite3 lm10.db -init scripts/lm10.sql -bail

update_organization: organization.csv update_filing
	cat $< | sqlite3 lm10.db -init scripts/organization.sql -bail

update_signature: signature.csv update_filing
	cat $< | sqlite3 lm10.db -init scripts/signature.sql -bail

update_other_address: other_address.csv update_filing
	cat $< | sqlite3 lm10.db -init scripts/other_address.sql -bail

update_principal_officer: principal_officer.csv update_filing
	cat $< | sqlite3 lm10.db -init scripts/principal_officer.sql -bail

update_reportable_activity: reportable_activity.csv update_filing
	cat $< | sqlite3 lm10.db -init scripts/reportable_activity.sql -bail

update_reporting_employer: reporting_employer.csv update_filing
	cat $< | sqlite3 lm10.db -init scripts/reporting_employer.sql -bail

update_filing: filing.csv update_filer
	cat $< | sqlite3 lm10.db -init scripts/filing.sql -bail

update_filer: filer.csv | lm10.db
	@if [ "$$(wc -l < $<)" -gt 1 ]; then \
	    cat $< | sqlite3 lm10.db -init scripts/filer.sql -bail; \
	else \
	    echo "update_filer: $< empty (spider produced no rows); keeping existing filer table" >&2; \
	fi

# ============================================================================
# CSV pipeline (consumers before producers)
# ============================================================================

# Per-table CSVs (consumed by update_<table>): sed renames from the
# json-to-multicsv output.

filing.csv: raw_filing.csv
	sed -r '1s/[a-z0-9_]+\.//g' $< > $@

lm10.csv: form.csv
	sed '1s/.*\._key/rptId/g' $< | sed -r '1s/[a-z0-9_]+\.//g' > $@

activity.csv: form.activity.csv
	sed '1s/form\.activity\._key/activity_id/g' $< \
	    | sed '1s/form\._key/rptId/g' \
	    | sed -r '1s/[a-z0-9_]+\.//g' > $@

counterparty_contact.csv: form.activity.counterparty_contact.csv
	sed '1s/form\.activity\._key/activity_id/g' $< \
	    | sed '1s/form\.activity\.counterparty_contact\._key/order/g' \
	    | sed '1s/form\._key/rptId/g' \
	    | sed -r '1s/[a-z0-9_]+\.//g' > $@

counterparty_organization.csv: form.activity.counterparty_organization.csv
	sed '1s/form\.activity\._key/activity_id/g' $< \
	    | sed '1s/form\.activity\.counterparty_organization\._key/order/g' \
	    | sed '1s/form\._key/rptId/g' \
	    | sed -r '1s/[a-z0-9_]+\.//g' > $@

expenditure.csv: form.activity.expenditure.csv
	sed '1s/form\.activity\._key/activity_id/g' $< \
	    | sed '1s/form\.activity\.expenditure\._key/order/g' \
	    | sed '1s/form\._key/rptId/g' \
	    | sed -r '1s/[a-z0-9_]+\.//g' > $@

# Pattern rule for simple per-rptId children: form.X.csv → X.csv.
# (signature, other_address, principal_officer, reportable_activity,
#  reporting_employer.) Explicit rules above take precedence for tables
# that need more renames.
%.csv: form.%.csv
	sed '1s/form\._key/rptId/g' $< | sed -r '1s/[a-z0-9_]+\.//g' > $@

# json-to-multicsv emits all FORM_CSVS from one invocation.
raw_filing.csv: filing.json
	json-to-multicsv --file filing.json --path /:table:raw_filing

$(FORM_CSVS) &: form.json
	json-to-multicsv --file form.json \
	    --path /:table:form \
	    --path /*/activity_details:table:activity \
	    --path /*/activity_details/*/counterparty_contact:table:counterparty_contact \
	    --path /*/activity_details/*/counterparty_organization:table:counterparty_organization \
	    --path /*/activity_details/*/expenditures:table:expenditure \
	    --path /*/other_address:table:other_address \
	    --path /*/principal_officer:table:principal_officer \
	    --path /*/reportable_activity:table:reportable_activity \
	    --path /*/reporting_employer:table:reporting_employer \
	    --path /*/signatures:table:signature \
	    --path /*/where_records:column

# jq fan-out from the spider's filing.jl.
filing.json: filing.jl
	jq -s '.[] | del(.detailed_form_data, .file_headers, .file_urls) | .files = .files[0] | .file_path = .files.path | .file_checksum = .files.checksum | .file_status = .files.status | del(.files)' $< \
	    | jq -s > $@

form.json: filing.jl
	jq -s '.[] | .detailed_form_data + {rptId, formFiled} | select(.file_number)' $< \
	    | jq -s \
	    | jq 'INDEX(.rptId) | with_entries(.value |= del(.rptId))' > $@

# Spider outputs (only invoked when sr_nums.txt is non-empty; the
# `update` recipe guards this).
filing.jl: sr_nums.txt
	scrapy crawl filings_incremental -L WARNING -a sr_nums_file=$< -O $@

organization.csv: sr_nums.txt
	scrapy crawl organizations_incremental -L WARNING -a sr_nums_file=$< -O $@

# Always-fresh inputs.
sr_nums.txt: FORCE lm10.db
	python scripts/discover_new_filings.py lm10.db > $@

filer.csv: FORCE
	scrapy crawl filers -L INFO -O $@

# Bootstrap. Fetch the prior nightly if no local lm10.db; fails fast
# on download error (recovery is a human-triggered `make lm10.db`).
lm10.db:
	curl -fsSL -o prev.zip $(PRIOR_DB_URL)
	unzip -o prev.zip lm10.db
	rm -f prev.zip

FORCE:

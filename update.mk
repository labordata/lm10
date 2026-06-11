# update.mk — incremental update of lm10.db.
#
# Discover filers with new LM-10 activity
# (scripts/discover_new_filings.py), crawl just those filers with the
# *_incremental spiders, run the same CSV transforms as the full build
# (common.mk), and merge each table into the previously released
# database with scripts/merge_csv.py. The filer table is refreshed on
# every run. Paper filings and upstream deletions this path can't see
# are reconciled by the scheduled full rebuild
# (.github/workflows/full-build.yml).
#
# Usage: make -f update.mk update

SHELL := /bin/bash
.SHELLFLAGS := -o pipefail -c

PRIOR_DB_URL ?= https://github.com/labordata/lm10/releases/download/nightly/lm10.db.zip

.DELETE_ON_ERROR:

MERGE_TARGETS := update_filing update_lm10 update_activity \
    update_counterparty_contact update_counterparty_organization \
    update_expenditure update_signature update_other_address \
    update_principal_officer update_reportable_activity \
    update_reporting_employer update_organization

.PHONY: update polish_db fk-check update_filer $(MERGE_TARGETS)

# ============================================================================
# Entry
# ============================================================================

# Discovery and the filer refresh must be fresh every run, so rebuild
# them once here, explicitly, rather than marking them FORCE — which
# would make any direct sub-target invocation re-crawl them mid-pipeline.
# The merge cascade (and the spider crawls feeding it) only runs when
# discovery found new filings.
update: lm10.db
	rm -f filer.csv sr_nums.txt
	$(MAKE) -f update.mk -j2 sr_nums.txt filer.csv
	$(MAKE) -f update.mk update_filer
	@if [ -s sr_nums.txt ]; then \
	    $(MAKE) -f update.mk $(MERGE_TARGETS) polish_db; \
	else \
	    echo "update: no new filings discovered; only the filer table was refreshed" >&2; \
	fi
	@$(MAKE) -f update.mk fk-check

# ============================================================================
# Validation
# ============================================================================

fk-check:
	@violations=$$(sqlite3 lm10.db "PRAGMA foreign_key_check;" 2>&1); \
	if [ -n "$$violations" ]; then \
	    echo "fk-check: violations in lm10.db:" >&2; \
	    echo "$$violations" >&2; \
	    echo "" >&2; \
	    echo "Detailed violations by table:" >&2; \
	    sqlite3 lm10.db "SELECT \"table\", COUNT(*) as violation_count FROM pragma_foreign_key_check() GROUP BY \"table\";" >&2; \
	    exit 1; \
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
	# drop staging tables a pre-merge_csv.py version of this pipeline
	# left in the published database
	for t in $$(sqlite3 lm10.db "select name from sqlite_master where type = 'table' and name like 'raw_%'"); do \
	    sqlite3 lm10.db "drop table \"$$t\""; \
	done

# ============================================================================
# Per-table merges
# ============================================================================

# Fields the full build's sqlite-utils transforms drop, mirrored here.
MERGE_FLAGS_filer := --replace
MERGE_FLAGS_filing := --ignore _key
MERGE_FLAGS_signature := --ignore _key
MERGE_FLAGS_other_address := --ignore _key
MERGE_FLAGS_principal_officer := --ignore _key
MERGE_FLAGS_reportable_activity := --ignore _key
MERGE_FLAGS_reporting_employer := --ignore _key
MERGE_FLAGS_counterparty_contact := --ignore order
MERGE_FLAGS_counterparty_organization := --ignore order
MERGE_FLAGS_expenditure := --ignore order

$(MERGE_TARGETS) update_filer: update_%: %.csv | lm10.db
	python scripts/merge_csv.py lm10.db $* $(MERGE_FLAGS_$*) < $<

# FK order: filing references filer; the lm10 merge clears the
# form-derived child tables, so it must precede their merges; activity
# must precede the children that resolve ordinals against its ids;
# organization needs filing's rptIds for its orphan filter.
update_filing: update_filer
update_lm10: update_filing
update_activity: update_lm10
update_counterparty_contact update_counterparty_organization update_expenditure: update_activity
update_signature update_other_address update_principal_officer \
    update_reportable_activity update_reporting_employer: update_lm10
update_organization: update_filing

# ============================================================================
# Spider outputs
# ============================================================================

# Every discovered filer was discovered FROM a filing, so its detail
# feed must yield at least one item; fewer items than filers means the
# crawl was blocked (OLMS 403s), not that there was nothing to fetch.
filing.jl: sr_nums.txt
	scrapy crawl filings_incremental -L INFO -a sr_nums_file=$< -O $@
	@[ "$$(wc -l < $@)" -ge "$$(wc -l < $<)" ] || \
	    (echo "ERROR: $@ has fewer filings than discovered filers; crawl was likely blocked" >&2 && exit 1)

organization.csv: sr_nums.txt
	scrapy crawl organizations_incremental -L INFO -a sr_nums_file=$< -O $@

sr_nums.txt: | lm10.db
	python scripts/discover_new_filings.py lm10.db > $@

# The filer list servlet always has thousands of filers; an empty crawl
# means something is broken (e.g. OLMS blocking us), not an empty list.
filer.csv:
	scrapy crawl filers -L INFO -O $@
	@[ "$$(wc -l < $@)" -gt 1 ] || (echo "ERROR: $@ is empty" >&2 && exit 1)

# Bootstrap. Fetch the prior nightly if no local lm10.db; fails fast
# on download error (recovery is a human-triggered full rebuild).
lm10.db:
	curl -fsSL -o prev.zip $(PRIOR_DB_URL)
	unzip -o prev.zip lm10.db
	rm -f prev.zip

include common.mk

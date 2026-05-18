# update.mk — incremental update of lm10.db.
#
# Discover filers (srNums) with new LM-10 activity since lm10.db's
# max(rptId), scrape just those, fan out via the same jq +
# json-to-multicsv + sed steps as the full build, then merge into
# lm10.db using temp tables in a single sqlite connection.
#
# Usage: make -f update.mk
#
# The filer list is refreshed unconditionally — the list endpoint is
# cheap and filer metadata can drift without a new filing.

PYTHON  ?= python
SCRAPY  ?= scrapy
JQ      ?= jq
JSON2CSV ?= json-to-multicsv

STAGING := staging

CACHE_OPTS = -s HTTPCACHE_ENABLED=True \
             -s HTTPCACHE_EXPIRATION_SECS=3600 \
             -s HTTPCACHE_DIR=.scrapy/httpcache

.PHONY: update incremental fan-out fetch-prior-db fk-check

PRIOR_DB_URL ?= https://github.com/labordata/lm10/releases/download/nightly/lm10.db.zip

# Self-deciding entry point: try to extend lm10.db incrementally; if
# nothing is available locally or upstream, fall through to a full
# crawl. The workflow only ever has to call `make -f update.mk update`.
update:
	@if [ ! -s lm10.db ]; then \
	    $(MAKE) -f update.mk fetch-prior-db || true; \
	fi
	@if [ -s lm10.db ]; then \
	    $(MAKE) -f update.mk incremental; \
	else \
	    echo "no lm10.db available; doing a full crawl"; \
	    $(MAKE) lm10.db; \
	fi
	@$(MAKE) -f update.mk fk-check

# Sanity gate: lm10.db must have no dangling foreign-key references
# regardless of which build path produced it.
fk-check:
	@violations=$$(sqlite3 lm10.db "PRAGMA foreign_key_check;"); \
	if [ -n "$$violations" ]; then \
	    echo "fk-check: violations in lm10.db:" >&2; \
	    echo "$$violations" >&2; \
	    exit 1; \
	fi
	@echo "fk-check: lm10.db has no FK violations"

# Pull the prior nightly release. Failure is non-fatal — `update` falls
# back to a full crawl if this step doesn't leave an lm10.db behind.
fetch-prior-db:
	curl -fsSL -o prev.zip $(PRIOR_DB_URL)
	unzip -o prev.zip lm10.db
	rm -f prev.zip

incremental:
	rm -rf $(STAGING) && mkdir -p $(STAGING)
	$(PYTHON) tools/discover_new_filings.py --max-known-from-db lm10.db \
	    > $(STAGING)/sr_nums.txt
	@N=$$(wc -l < $(STAGING)/sr_nums.txt | tr -d ' '); \
	    echo "incremental: $$N filers with new activity"
	$(SCRAPY) crawl filers -L WARNING -O $(STAGING)/filer.csv
	@if [ -s $(STAGING)/sr_nums.txt ]; then \
	    $(MAKE) -f update.mk fan-out; \
	fi
	$(PYTHON) tools/merge_incremental.py lm10.db $(STAGING)
	rm -rf $(STAGING)

fan-out:
	rm -rf .scrapy/httpcache
	$(SCRAPY) crawl filings_incremental -L WARNING $(CACHE_OPTS) \
	    -a sr_nums_file=$(STAGING)/sr_nums.txt \
	    -O $(STAGING)/filing.jl
	$(SCRAPY) crawl organizations_incremental -L WARNING $(CACHE_OPTS) \
	    -a sr_nums_file=$(STAGING)/sr_nums.txt \
	    -O $(STAGING)/organization.csv
	rm -rf .scrapy/httpcache
	@if [ ! -s $(STAGING)/filing.jl ]; then \
	    echo "incremental: filings spider produced no new filings"; \
	    exit 0; \
	fi
	cat $(STAGING)/filing.jl | $(JQ) -s \
	    '.[] | .detailed_form_data + {rptId, formFiled} | select(.file_number)' \
	    | $(JQ) -s \
	    | $(JQ) 'INDEX(.rptId) | with_entries(.value |= del(.rptId))' \
	    > $(STAGING)/form.json
	cat $(STAGING)/filing.jl | $(JQ) -s \
	    '.[] | del(.detailed_form_data, .file_headers, .file_urls) | .files = .files[0] | .file_path = .files.path | .file_checksum = .files.checksum | .file_status = .files.status | del(.files)' \
	    | $(JQ) -s > $(STAGING)/filing.json
	cd $(STAGING) && $(JSON2CSV) --file form.json \
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
	cd $(STAGING) && $(JSON2CSV) --file filing.json --path /:table:raw_filing
	# Apply the same sed renames as Makefile, chained via pipes.
	cat $(STAGING)/raw_filing.csv \
	    | sed -r '1s/[a-z0-9_]+\.//g' > $(STAGING)/filing.csv
	cat $(STAGING)/form.csv \
	    | sed '1s/.*\._key/rptId/g' \
	    | sed -r '1s/[a-z0-9_]+\.//g' > $(STAGING)/lm10.csv
	cat $(STAGING)/form.activity.counterparty_contact.csv \
	    | sed '1s/form\.activity\._key/activity_id/g' \
	    | sed '1s/form\.activity\.counterparty_contact\._key/order/g' \
	    | sed '1s/form\._key/rptId/g' \
	    | sed -r '1s/[a-z0-9_]+\.//g' > $(STAGING)/counterparty_contact.csv
	cat $(STAGING)/form.activity.counterparty_organization.csv \
	    | sed '1s/form\.activity\._key/activity_id/g' \
	    | sed '1s/form\.activity\.counterparty_organization\._key/order/g' \
	    | sed '1s/form\._key/rptId/g' \
	    | sed -r '1s/[a-z0-9_]+\.//g' > $(STAGING)/counterparty_organization.csv
	cat $(STAGING)/form.activity.csv \
	    | sed '1s/form\.activity\._key/activity_id/g' \
	    | sed '1s/form\._key/rptId/g' \
	    | sed -r '1s/[a-z0-9_]+\.//g' > $(STAGING)/activity.csv
	cat $(STAGING)/form.activity.expenditure.csv \
	    | sed '1s/form\.activity\._key/activity_id/g' \
	    | sed '1s/form\.activity\.expenditure\._key/order/g' \
	    | sed '1s/form\._key/rptId/g' \
	    | sed -r '1s/[a-z0-9_]+\.//g' > $(STAGING)/expenditure.csv
	for t in other_address principal_officer reportable_activity reporting_employer signature; do \
	    cat $(STAGING)/form.$$t.csv \
	        | sed '1s/form\._key/rptId/g' \
	        | sed -r '1s/[a-z0-9_]+\.//g' > $(STAGING)/$$t.csv ; \
	done

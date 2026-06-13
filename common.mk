# common.mk — CSV/JSON transform rules shared by the full build
# (Makefile) and the incremental update (update.mk). The spider targets
# that produce filing.jl, filer.csv, and organization.csv live in the
# including makefile.

FORM_CSVS := form.csv form.activity.csv \
    form.activity.counterparty_contact.csv \
    form.activity.counterparty_organization.csv \
    form.activity.expenditure.csv \
    form.other_address.csv form.principal_officer.csv \
    form.reportable_activity.csv form.reporting_employer.csv \
    form.signature.csv

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

# json-to-multicsv only writes files for tables that received at least
# one row, so touch the declared outputs: a batch with, say, no
# expenditures then yields an empty CSV downstream ("no rows to merge")
# instead of a missing prerequisite.
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
	touch $(FORM_CSVS)

raw_filing.csv: filing.json
	json-to-multicsv --file filing.json --path /:table:raw_filing
	touch $@

filing.json: filing.jl
	jq -s '.[] | del(.detailed_form_data, .file_headers, .file_urls) | .files = .files[0] | .file_path = .files.path | .file_checksum = .files.checksum | .file_status = .files.status | del(.files)' $< \
	    | jq -s > $@

form.json: filing.jl
	jq -s '.[] | .detailed_form_data + {rptId, formFiled} | select(.file_number)' $< \
	    | jq -s \
	    | jq 'INDEX(.rptId) | with_entries(.value |= del(.rptId))' > $@

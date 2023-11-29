lm10.db : filing.csv counterparty_contact.csv				\
          counterparty_organization.csv activity.csv expenditure.csv	\
          lm10.csv other_address.csv principal_officer.csv		\
          reportable_activity.csv reporting_employer.csv		\
          signature.csv
	csvs-to-sqlite $^ $@
	sqlite-utils $@ 'delete from counterparty_contact where city is null and name is null and "po_box,_bldg,_room_no,_if_any" is null and state is null and street is null and "zip_code_+_4" is null'

filing.csv : raw_filing.csv
	cat $< | \
            sed '1s/.*\._key/rptId/g' | \
	    sed -r '1s/[a-z0-9_]+\.//g' > $@

lm10.csv : form.csv
	cat $< | \
            sed '1s/.*\._key/rptId/g' | \
	    sed -r '1s/[a-z0-9_]+\.//g' > $@


counterparty_contact.csv : form.activity.counterparty_contact.csv
	cat $< | \
            sed '1s/form\.activity\._key/activity_id/g' | \
	    sed '1s/form\.activity\.counterparty_contact\._key/order/g' | \
            sed '1s/form\._key/rptId/g' | \
	    sed -r '1s/[a-z0-9_]+\.//g' > $@

counterparty_organization.csv : form.activity.counterparty_organization.csv 
	cat $< | \
            sed '1s/form\.activity\._key/activity_id/g' | \
	    sed '1s/form\.activity\.counterparty_organization\._key/order/g' | \
            sed '1s/form\._key/rptId/g' | \
	    sed -r '1s/[a-z0-9_]+\.//g' > $@

activity.csv : form.activity.csv
	cat $< | \
            sed '1s/form\.activity\._key/activity_id/g' | \
            sed '1s/form\._key/rptId/g' | \
	    sed -r '1s/[a-z0-9_]+\.//g' > $@

expenditure.csv : form.activity.expenditure.csv
	cat $< | \
            sed '1s/form\.activity\._key/activity_id/g' | \
	    sed '1s/form\.activity\.expenditure\._key/order/g' | \
            sed '1s/form\._key/rptId/g' | \
	    sed -r '1s/[a-z0-9_]+\.//g' > $@

other_address.csv : form.other_address.csv
	cat $< | \
            sed '1s/form\._key/rptId/g' | \
	    sed -r '1s/[a-z0-9_]+\.//g' > $@

principal_officer.csv : form.principal_officer.csv
	cat $< | \
            sed '1s/form\._key/rptId/g' | \
	    sed -r '1s/[a-z0-9_]+\.//g' > $@

reportable_activity.csv : form.reportable_activity.csv
	cat $< | \
            sed '1s/form\._key/rptId/g' | \
	    sed -r '1s/[a-z0-9_]+\.//g' > $@

reporting_employer.csv : form.reporting_employer.csv
	cat $< | \
            sed '1s/form\._key/rptId/g' | \
	    sed -r '1s/[a-z0-9_]+\.//g' > $@

signature.csv : form.signature.csv
	cat $< | \
            sed '1s/form\._key/rptId/g' | \
	    sed -r '1s/[a-z0-9_]+\.//g' > $@

form.activity.counterparty_contact.csv form.activity.counterparty_organization.csv form.activity.csv form.activity.expenditure.csv form.csv form.other_address.csv form.principal_officer.csv form.reportable_activity.csv form.reporting_employer.csv form.signature.csv : form.json
	json-to-multicsv.pl --file $< \
            --path /:table:form \
            --path /*/activity_details/:table:activity \
            --path /*/activity_details/*/counterparty_contact/:table:counterparty_contact \
            --path /*/activity_details/*/counterparty_organization/:table:counterparty_organization \
            --path /*/activity_details/*/expenditures/:table:expenditure \
            --path /*/other_address/:table:other_address \
            --path /*/principal_officer/:table:principal_officer \
            --path /*/reportable_activity/:table:reportable_activity \
            --path /*/reporting_employer/:table:reporting_employer \
            --path /*/signatures/:table:signature \
            --path /*/where_records/:column

raw_filing.csv : filing.json
	json-to-multicsv.pl --file filing.json --path /:table:raw_filing

form.json : filing.jl
	cat $< |  jq -s '.[] | .detailed_form_data + {rptId, formFiled} | select(.file_number)' | jq -s | jq 'INDEX(.rptId) | with_entries(.value |= del(.rptId))' > $@


filing.json : filing.jl
	cat $< | jq -s '.[] | del(.detailed_form_data, .file_headers, .file_urls) | .files = .files[0]' | jq -s > $@

filer.csv :
	scrapy crawl filers -L 'WARNING' -O $@

filing.jl :
	scrapy crawl filings -L 'WARNING' -O $@

organization.csv :
	scrapy crawl organizations -L 'WARNING' -O $@

lm10.db : filing.csv counterparty_contact.csv				\
          counterparty_organization.csv activity.csv expenditure.csv	\
          lm10.csv other_address.csv principal_officer.csv		\
          reportable_activity.csv reporting_employer.csv		\
          signature.csv organization.csv filer.csv
	csvs-to-sqlite $^ $@
	sqlite-utils $@ 'delete from counterparty_contact where city is null and name is null and "po_box,_bldg,_room_no,_if_any" is null and state is null and street is null and "zip_code_+_4" is null'
	sqlite-utils $@ 'delete from principal_officer where city is null and name is null and "po_box,_bldg,_room_no,_if_any" is null and state is null and street is null and "zip_code_+_4" is null'
	sqlite-utils $@ 'delete from other_address where city is null and name is null and "po_box,_bldg,_room_no,_if_any" is null and state is null and street is null and "zip_code_+_4" is null'
	sqlite-utils transform $@ filer \
          --pk srNum
	sqlite-utils transform $@ organization \
          --drop amended \
          --drop beginDate \
          --drop endDate \
          --drop registerDate \
          --drop formFiled \
          --drop receiveDate \
          --drop srFilerId \
          --drop amendment \
          --drop empTrdName \
          --drop subLabOrg1 \
          --drop subLabOrg2 \
          --drop formLink \
          --drop srNum \
          --drop repOrgsCnt \
          --drop paperOrElect \
          --drop address1 \
          --drop address2 \
          --drop zip \
          --drop attachmentId \
          --drop fileName \
          --drop fileDesc \
          --drop yrCovered
	sqlite-utils transform $@ filing \
          --pk rptId
	sqlite-utils convert $@ lm10 period_begin 'r.parsedate(value)'
	sqlite-utils convert $@ lm10 period_through 'r.parsedate(value)'
	sqlite-utils convert $@ expenditure date 'r.parsedate(value)'
	sqlite-utils convert $@ signature on_date 'r.parsedate(value)'
	sqlite-utils transform $@ activity \
          --pk id
	sqlite-utils convert $@ activity date_of_agreement 'r.parsedate(value) if value.lower() != "none" else None'
	sqlite-utils convert $@ organization promiseDate 'r.parsedate(value) if value.lower() not in {"not available", "none"} else None'
	sqlite-utils $@ "update counterparty_contact set activity_id = (select id from activity where rptId = counterparty_contact.rptId and activity_id = counterparty_contact.activity_id)"
	sqlite-utils $@ "update counterparty_organization set activity_id = (select id from activity where rptId = counterparty_organization.rptId and activity_id = counterparty_organization.activity_id)"
	sqlite-utils $@ "update expenditure set activity_id = (select id from activity where rptId = expenditure.rptId and activity_id = expenditure.activity_id)"
	sqlite-utils transform $@ activity \
          --drop activity_id
	sqlite-utils transform $@ counterparty_contact \
          --drop rptId \
          --drop order
	sqlite-utils transform $@ counterparty_organization \
          --drop rptId \
          --drop order
	sqlite-utils transform $@ expenditure \
          --drop rptId \
          --drop order
	sqlite-utils transform $@ filing \
          --drop _key
	sqlite-utils transform $@ other_address \
          --drop _key
	sqlite-utils transform $@ principal_officer \
          --drop _key
	sqlite-utils transform $@ reportable_activity \
          --drop _key
	sqlite-utils transform $@ reporting_employer \
          --drop _key
	sqlite-utils transform $@ signature \
          --drop _key
	sqlite-utils add-foreign-keys $@ \
          activity rptId filing rptId \
          counterparty_contact activity_id activity id \
          counterparty_organization activity_id activity id \
          expenditure activity_id activity id \
          lm10 rptId filing rptId \
          other_address rptId filing rptId \
          principal_officer rptId filing rptId \
          reportable_activity rptId filing rptId \
          reporting_employer rptId filing rptId \
          signature rptId filing rptId \
          organization rptId filing rptId \
          filing srNum filer srNum

filing.csv : raw_filing.csv
	cat $< | \
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
	cat $< | jq -s '.[] | del(.detailed_form_data, .file_headers, .file_urls) | .files = .files[0] | .file_path = .files.path | .file_checksum = .files.checksum | .file_status = .files.status | del(.files)' | jq -s > $@

filer.csv :
	scrapy crawl filers -L 'WARNING' -O $@

filing.jl :
	scrapy crawl filings -L 'WARNING' -O $@

organization.csv :
	scrapy crawl organizations -L 'WARNING' -O $@

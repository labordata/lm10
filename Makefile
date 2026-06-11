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

filer.csv :
	scrapy crawl filers -L 'WARNING' -O $@

filing.jl :
	scrapy crawl filings -L 'WARNING' -O $@

organization.csv :
	scrapy crawl organizations -L 'WARNING' -O $@

include common.mk

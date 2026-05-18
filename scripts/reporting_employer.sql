BEGIN;
DROP TABLE IF EXISTS raw_reporting_employer;
.mode csv
.import /dev/stdin raw_reporting_employer

DELETE FROM reporting_employer WHERE rptId IN (SELECT DISTINCT rptId FROM raw_reporting_employer);

INSERT INTO reporting_employer (
    rptId, attention_to, city, employer, "po_box,_bldg,_room_no,_if_any",
    state, street, title, trade_name, "zip_code_+_4"
)
SELECT
    rptId, attention_to, city, employer, "po_box,_bldg,_room_no,_if_any",
    state, street, title, trade_name, "zip_code_+_4"
FROM raw_reporting_employer;

SELECT changes() || ' rows inserted into reporting_employer';
END;

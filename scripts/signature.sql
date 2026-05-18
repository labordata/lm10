BEGIN;
DROP TABLE IF EXISTS raw_signature;
.mode csv
.import /dev/stdin raw_signature

DELETE FROM signature
WHERE
  rptId IN (SELECT DISTINCT rptId FROM raw_signature);

INSERT INTO signature(rptId, on_date, signed, telephone_number, title)
SELECT rptId, on_date, signed, telephone_number, title FROM raw_signature;

SELECT changes() || ' rows inserted into signature';

COMMIT;

BEGIN;
DROP TABLE IF EXISTS raw_lm10;
.mode csv
.import /dev/stdin raw_lm10

DELETE FROM lm10 WHERE rptId IN (SELECT DISTINCT rptId FROM raw_lm10);

INSERT INTO lm10(
  rptId,
  file_number,
  formFiled,
  period_begin,
  period_through,
  type_of_organization,
  records_hosted_at_other_address,
  records_hosted_with_principal_officer,
  records_hosted_with_reporting_employer
)
SELECT
  rptId,
  file_number,
  formFiled,
  period_begin,
  period_through,
  type_of_organization,
  records_hosted_at_other_address,
  records_hosted_with_principal_officer,
  records_hosted_with_reporting_employer
FROM raw_lm10;

SELECT changes() || ' rows inserted into lm10';

COMMIT;

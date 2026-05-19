BEGIN;
DROP TABLE IF EXISTS raw_organization;
.mode csv
.import /dev/stdin raw_organization

DELETE FROM organization
WHERE
  rptId IN (SELECT DISTINCT rptId FROM raw_organization);

-- Skip org rows for rptIds we don't have in filing. The filings
-- spider hits orgReport.do per filing, which sometimes 403s and
-- drops the filing item; the organizations spider goes through a
-- different endpoint (GetAdditionalEmpsLM10Servlet) that's less
-- flaky, so it can return org data for filings the filings spider
-- missed. Filtering here avoids FK violations; a future full crawl
-- will pick up the missing org rows once the filing lands.
INSERT INTO organization(promiseDate, oID, rptId, empLabOrg, city, state)
SELECT promiseDate, oID, rptId, empLabOrg, city, state
FROM raw_organization
WHERE rptId IN (SELECT rptId FROM filing);

SELECT changes() || ' rows inserted into organization';

SELECT
  'organization: dropped '
  || (SELECT COUNT(*) FROM raw_organization WHERE rptId NOT IN (SELECT rptId FROM filing))
  || ' rows whose rptId is not in filing';

COMMIT;

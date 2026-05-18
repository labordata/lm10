BEGIN;
DROP TABLE IF EXISTS raw_organization;
.mode csv
.import /dev/stdin raw_organization

DELETE FROM organization WHERE rptId IN (SELECT DISTINCT rptId FROM raw_organization);

INSERT INTO organization (promiseDate, oID, rptId, empLabOrg, city, state)
SELECT promiseDate, oID, rptId, empLabOrg, city, state FROM raw_organization;

SELECT changes() || ' rows inserted into organization';
END;

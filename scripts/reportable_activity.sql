BEGIN;
DROP TABLE IF EXISTS raw_reportable_activity;
.mode csv
.import /dev/stdin raw_reportable_activity

DELETE FROM reportable_activity WHERE rptId IN (SELECT DISTINCT rptId FROM raw_reportable_activity);

INSERT INTO reportable_activity (rptId, answer, code, n_responses, question)
SELECT rptId, answer, code, n_responses, question FROM raw_reportable_activity;

SELECT changes() || ' rows inserted into reportable_activity';
END;

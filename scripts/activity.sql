BEGIN;
DROP TABLE IF EXISTS raw_activity;
.mode csv
.import /dev/stdin raw_activity

-- Cascade-delete child rows for the new rptIds BEFORE we touch
-- activity. After we delete activity rows their ids can be reused by
-- the INSERT below, so a post-hoc "WHERE activity_id NOT IN activity"
-- check would silently miss orphans whose ids got reallocated.
DELETE FROM counterparty_contact      WHERE activity_id IN (SELECT id FROM activity WHERE rptId IN (SELECT DISTINCT rptId FROM raw_activity));
DELETE FROM counterparty_organization WHERE activity_id IN (SELECT id FROM activity WHERE rptId IN (SELECT DISTINCT rptId FROM raw_activity));
DELETE FROM expenditure               WHERE activity_id IN (SELECT id FROM activity WHERE rptId IN (SELECT DISTINCT rptId FROM raw_activity));

DELETE FROM activity WHERE rptId IN (SELECT DISTINCT rptId FROM raw_activity);

-- Insert in deterministic (rptId, ordinal) order so child scripts can
-- recover the ordinal via ROW_NUMBER() OVER (PARTITION BY rptId ORDER BY id).
INSERT INTO activity (
    "12b_exists", activity_code, activity_type, agencies,
    counterparty_position, date_of_agreement, explanation, federal_work,
    form_agreement, no_uei_checkbox, uei, unlisted_agencies, rptId
)
SELECT
    "12b_exists", activity_code, activity_type, agencies,
    counterparty_position, date_of_agreement, explanation, federal_work,
    form_agreement, no_uei_checkbox, uei, unlisted_agencies, rptId
FROM raw_activity
ORDER BY CAST(rptId AS INTEGER), CAST(activity_id AS INTEGER);

SELECT changes() || ' rows inserted into activity';
END;

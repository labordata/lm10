BEGIN;
DROP TABLE IF EXISTS raw_expenditure;
.mode csv
.import /dev/stdin raw_expenditure

-- activity.sql has already cleaned up old child rows for these rptIds.
INSERT INTO expenditure (activity_id, amount, date, kind)
SELECT a.id, raw.amount, raw.date, raw.kind
FROM raw_expenditure raw
JOIN (
    SELECT id, rptId,
           ROW_NUMBER() OVER (PARTITION BY rptId ORDER BY id) - 1 AS ordinal
      FROM activity
     WHERE rptId IN (SELECT DISTINCT CAST(rptId AS INTEGER) FROM raw_expenditure)
) a ON a.rptId = CAST(raw.rptId AS INTEGER)
   AND a.ordinal = CAST(raw.activity_id AS INTEGER);

SELECT changes() || ' rows inserted into expenditure';
END;

BEGIN;
DROP TABLE IF EXISTS raw_counterparty_organization;
.mode csv
.import /dev/stdin raw_counterparty_organization

-- activity.sql has already cleaned up old child rows for these rptIds.
INSERT INTO counterparty_organization(
  activity_id,
  city,
  organization,
  "po_box,_bldg,_room_no,_if_any",
  state,
  street,
  "zip_code_+_4"
)
SELECT
  a.id,
  raw.city,
  raw.organization,
  raw."po_box,_bldg,_room_no,_if_any",
  raw.state,
  raw.street,
  raw."zip_code_+_4"
FROM raw_counterparty_organization AS raw
JOIN (
  SELECT
    id,
    rptId,
    ROW_NUMBER() OVER (PARTITION BY rptId ORDER BY id) - 1 AS ordinal
  FROM activity
  WHERE
    rptId IN (
      SELECT DISTINCT CAST(rptId AS INTEGER)
      FROM raw_counterparty_organization
    )
) AS a
  ON a.rptId = CAST(raw.rptId AS INTEGER)
  AND a.ordinal = CAST(raw.activity_id AS INTEGER);

SELECT changes() || ' rows inserted into counterparty_organization';

COMMIT;

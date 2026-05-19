BEGIN;
DROP TABLE IF EXISTS raw_counterparty_contact;
.mode csv
.import /dev/stdin raw_counterparty_contact

-- activity.sql has already cleaned up old child rows for these rptIds.
-- Resolve the per-rptId activity_id ordinal against main.activity via
-- ROW_NUMBER, then insert.
INSERT INTO counterparty_contact(
  activity_id,
  city,
  name,
  "po_box,_bldg,_room_no,_if_any",
  state,
  street,
  "zip_code_+_4"
)
SELECT
  a.id,
  raw.city,
  raw.name,
  raw."po_box,_bldg,_room_no,_if_any",
  raw.state,
  raw.street,
  raw."zip_code_+_4"
FROM raw_counterparty_contact AS raw
JOIN (
  SELECT
    id,
    rptId,
    ROW_NUMBER() OVER (PARTITION BY rptId ORDER BY id) - 1 AS ordinal
  FROM activity
  WHERE
    rptId IN (
      SELECT DISTINCT CAST(rptId AS INTEGER) FROM raw_counterparty_contact
    )
) AS a
  ON a.rptId = CAST(raw.rptId AS INTEGER)
  AND a.ordinal = CAST(raw.activity_id AS INTEGER);

DELETE FROM counterparty_contact
WHERE
  city IS NULL
  AND name IS NULL
  AND "po_box,_bldg,_room_no,_if_any" IS NULL
  AND state IS NULL
  AND street IS NULL
  AND "zip_code_+_4" IS NULL;

SELECT changes() || ' rows inserted into counterparty_contact';

COMMIT;

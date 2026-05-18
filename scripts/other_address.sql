BEGIN;
DROP TABLE IF EXISTS raw_other_address;
.mode csv
.import /dev/stdin raw_other_address

DELETE FROM other_address WHERE rptId IN (SELECT DISTINCT rptId FROM raw_other_address);

INSERT INTO other_address (
    rptId, city, name, organization, "po_box,_bldg,_room_no,_if_any",
    state, street, title, "zip_code_+_4"
)
SELECT
    rptId, city, name, organization, "po_box,_bldg,_room_no,_if_any",
    state, street, title, "zip_code_+_4"
FROM raw_other_address;

DELETE FROM other_address
 WHERE city IS NULL AND name IS NULL
   AND "po_box,_bldg,_room_no,_if_any" IS NULL
   AND state IS NULL AND street IS NULL AND "zip_code_+_4" IS NULL;

SELECT changes() || ' rows inserted into other_address';
END;

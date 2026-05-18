BEGIN;
DROP TABLE IF EXISTS raw_principal_officer;
.mode csv
.import /dev/stdin raw_principal_officer

DELETE FROM principal_officer WHERE rptId IN (SELECT DISTINCT rptId FROM raw_principal_officer);

INSERT INTO principal_officer (
    rptId, city, name, "po_box,_bldg,_room_no,_if_any",
    state, street, "zip_code_+_4"
)
SELECT
    rptId, city, name, "po_box,_bldg,_room_no,_if_any",
    state, street, "zip_code_+_4"
FROM raw_principal_officer;

DELETE FROM principal_officer
 WHERE city IS NULL AND name IS NULL
   AND "po_box,_bldg,_room_no,_if_any" IS NULL
   AND state IS NULL AND street IS NULL AND "zip_code_+_4" IS NULL;

SELECT changes() || ' rows inserted into principal_officer';
END;

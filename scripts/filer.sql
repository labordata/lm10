BEGIN;
DROP TABLE IF EXISTS raw_filer;
.mode csv
.import /dev/stdin raw_filer

INSERT OR REPLACE INTO filer (filerType, srFilerId, srNum, companyName, companyCity, companyState)
SELECT filerType, srFilerId, srNum, companyName, companyCity, companyState FROM raw_filer;

SELECT changes() || ' rows upserted into filer';
END;

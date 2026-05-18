BEGIN;
DROP TABLE IF EXISTS raw_filing;
.mode csv
.import /dev/stdin raw_filing

INSERT OR REPLACE INTO filing (
    address1, address2, amended, amendment, beginDate, city,
    empLabOrg, empTrdName, endDate, file_checksum, file_path, file_status,
    filing_url, formFiled, formLink, oID, paperOrElect, promiseDate,
    receiveDate, registerDate, repOrgsCnt, rptId, srFilerId, srNum,
    state, subLabOrg1, subLabOrg2, yrCovered, zip
)
SELECT
    address1, address2, amended, amendment, beginDate, city,
    empLabOrg, empTrdName, endDate, file_checksum, file_path, file_status,
    filing_url, formFiled, formLink, oID, paperOrElect, promiseDate,
    receiveDate, registerDate, repOrgsCnt, rptId, srFilerId, srNum,
    state, subLabOrg1, subLabOrg2, yrCovered, zip
FROM raw_filing;

SELECT changes() || ' rows upserted into filing';
END;

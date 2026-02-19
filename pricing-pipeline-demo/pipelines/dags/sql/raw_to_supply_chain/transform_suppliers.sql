-- =====================================================================
-- Transform: RAW.SAP_VENDORS -> SUPPLY_CHAIN.SUPPLIERS
-- Schedule: Daily @ 06:00 UTC (after Fivetran SAP sync)
-- =====================================================================
-- Transforms raw SAP vendor master data into the clean supplier
-- dimension. Filters out vendors marked for deletion.

MERGE INTO ENTERPRISE_DW.SUPPLY_CHAIN.SUPPLIERS AS tgt
USING (
    SELECT
        'SUP-' || RIGHT(VENDOR_NUMBER, 3)                  AS SUPPLIER_ID,
        VENDOR_NAME_1                                       AS SUPPLIER_NAME,
        -- Map ISO country codes to full names
        CASE
            WHEN COUNTRY_KEY = 'MY' THEN 'Malaysia'
            WHEN COUNTRY_KEY = 'US' THEN 'United States'
            WHEN COUNTRY_KEY = 'JP' THEN 'Japan'
            WHEN COUNTRY_KEY = 'IT' THEN 'Italy'
            WHEN COUNTRY_KEY = 'CN' THEN 'China'
            WHEN COUNTRY_KEY = 'KR' THEN 'South Korea'
            ELSE COUNTRY_KEY
        END                                                 AS COUNTRY,
        TRY_CAST(AVG_LEAD_TIME_DAYS AS INTEGER)            AS LEAD_TIME_DAYS,
        -- Map SAP payment terms codes
        CASE
            WHEN PAYMENT_TERMS_KEY = 'Z030' THEN 'NET-30'
            WHEN PAYMENT_TERMS_KEY = 'Z045' THEN 'NET-45'
            WHEN PAYMENT_TERMS_KEY = 'Z060' THEN 'NET-60'
            ELSE 'NET-30'
        END                                                 AS PAYMENT_TERMS,
        CASE
            WHEN DELETION_FLAG = 'X' THEN 'INACTIVE'
            ELSE 'ACTIVE'
        END                                                 AS STATUS
    FROM ENTERPRISE_DW.RAW.SAP_VENDORS
    WHERE DELETION_FLAG != 'X' OR DELETION_FLAG IS NULL OR DELETION_FLAG = ''
) AS src
ON tgt.SUPPLIER_ID = src.SUPPLIER_ID
WHEN MATCHED THEN UPDATE SET
    tgt.SUPPLIER_NAME  = src.SUPPLIER_NAME,
    tgt.COUNTRY        = src.COUNTRY,
    tgt.LEAD_TIME_DAYS = src.LEAD_TIME_DAYS,
    tgt.PAYMENT_TERMS  = src.PAYMENT_TERMS,
    tgt.STATUS         = src.STATUS
WHEN NOT MATCHED THEN INSERT (
    SUPPLIER_ID, SUPPLIER_NAME, COUNTRY, LEAD_TIME_DAYS, PAYMENT_TERMS, STATUS
) VALUES (
    src.SUPPLIER_ID, src.SUPPLIER_NAME, src.COUNTRY,
    src.LEAD_TIME_DAYS, src.PAYMENT_TERMS, src.STATUS
);

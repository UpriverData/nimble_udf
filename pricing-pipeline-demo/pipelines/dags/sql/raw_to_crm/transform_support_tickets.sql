-- =====================================================================
-- Transform: RAW.SALESFORCE_CASES -> CRM.SUPPORT_TICKETS
-- Schedule: Every 6 hours (runs after transform_customers)
-- =====================================================================
-- Transforms raw Salesforce cases into the clean support tickets table.
-- Joins to CRM.CUSTOMERS to resolve account ID -> customer ID mapping.
-- Only includes product-related cases (excludes general inquiries).

MERGE INTO ENTERPRISE_DW.CRM.SUPPORT_TICKETS AS tgt
USING (
    SELECT
        'TKT-' || RIGHT(sf.SF_CASE_ID, 4)                AS TICKET_ID,
        c.CUSTOMER_ID,
        CASE
            WHEN sf.PRODUCT_SKU IS NOT NULL
            THEN 'SKU-' || LPAD(sf.PRODUCT_SKU, 5, '0')
            ELSE NULL
        END                                                AS SKU_ID,
        sf.CASE_TYPE                                       AS ISSUE_TYPE,
        UPPER(sf.CASE_PRIORITY)                            AS PRIORITY,
        CASE
            WHEN UPPER(sf.CASE_STATUS) = 'CLOSED' THEN 'RESOLVED'
            WHEN UPPER(sf.CASE_STATUS) = 'OPEN'   THEN 'OPEN'
            ELSE UPPER(sf.CASE_STATUS)
        END                                                AS STATUS,
        TRY_TO_TIMESTAMP_NTZ(sf.CREATED_DATE)             AS CREATED_AT,
        TRY_TO_TIMESTAMP_NTZ(sf.CLOSED_DATE)              AS RESOLVED_AT
    FROM ENTERPRISE_DW.RAW.SALESFORCE_CASES sf
    LEFT JOIN ENTERPRISE_DW.CRM.CUSTOMERS c
        ON c.CUSTOMER_ID = 'CUST-' || REPLACE(sf.SF_ACCOUNT_ID, '001Dn00000A', '200')
    WHERE sf.CASE_TYPE != 'Information'
      AND sf.PRODUCT_SKU IS NOT NULL
) AS src
ON tgt.TICKET_ID = src.TICKET_ID
WHEN MATCHED THEN UPDATE SET
    tgt.CUSTOMER_ID = src.CUSTOMER_ID,
    tgt.SKU_ID      = src.SKU_ID,
    tgt.ISSUE_TYPE  = src.ISSUE_TYPE,
    tgt.PRIORITY    = src.PRIORITY,
    tgt.STATUS      = src.STATUS,
    tgt.CREATED_AT  = src.CREATED_AT,
    tgt.RESOLVED_AT = src.RESOLVED_AT
WHEN NOT MATCHED THEN INSERT (
    TICKET_ID, CUSTOMER_ID, SKU_ID, ISSUE_TYPE, PRIORITY, STATUS, CREATED_AT, RESOLVED_AT
) VALUES (
    src.TICKET_ID, src.CUSTOMER_ID, src.SKU_ID, src.ISSUE_TYPE,
    src.PRIORITY, src.STATUS, src.CREATED_AT, src.RESOLVED_AT
);

-- =====================================================================
-- Transform: RAW.NETSUITE_GL -> FINANCE.GL_ACCOUNTS
-- Schedule: Daily @ 05:00 UTC (after Fivetran NetSuite sync)
-- =====================================================================
-- Transforms raw NetSuite chart of accounts into the clean GL table.
-- Filters out summary accounts and inactive accounts.

MERGE INTO ENTERPRISE_DW.FINANCE.GL_ACCOUNTS AS tgt
USING (
    SELECT
        'GL-' || ACCOUNT_NUMBER                           AS ACCOUNT_ID,
        -- Clean up account names (remove " - Direct" suffixes for display)
        REGEXP_REPLACE(ACCOUNT_NAME, '\\s*-\\s*(Direct|Marketplace)$', '')
                                                          AS ACCOUNT_NAME,
        -- Map NetSuite account type names to our internal types
        CASE
            WHEN ACCOUNT_TYPE_NAME = 'Income'        THEN 'REVENUE'
            WHEN ACCOUNT_TYPE_NAME = 'Cost of Sales' THEN 'EXPENSE'
            WHEN ACCOUNT_TYPE_NAME = 'Expense'       THEN 'EXPENSE'
            WHEN ACCOUNT_TYPE_NAME = 'Other Income'  THEN 'CONTRA'
            ELSE UPPER(ACCOUNT_TYPE_NAME)
        END                                               AS ACCOUNT_TYPE,
        DEPARTMENT_NAME                                   AS DEPARTMENT
    FROM ENTERPRISE_DW.RAW.NETSUITE_GL
    WHERE IS_SUMMARY = FALSE
      AND IS_INACTIVE = FALSE
      AND ACCOUNT_TYPE_NAME IN ('Income', 'Cost of Sales', 'Expense', 'Other Income')
) AS src
ON tgt.ACCOUNT_ID = src.ACCOUNT_ID
WHEN MATCHED THEN UPDATE SET
    tgt.ACCOUNT_NAME = src.ACCOUNT_NAME,
    tgt.ACCOUNT_TYPE = src.ACCOUNT_TYPE,
    tgt.DEPARTMENT   = src.DEPARTMENT
WHEN NOT MATCHED THEN INSERT (
    ACCOUNT_ID, ACCOUNT_NAME, ACCOUNT_TYPE, DEPARTMENT
) VALUES (
    src.ACCOUNT_ID, src.ACCOUNT_NAME, src.ACCOUNT_TYPE, src.DEPARTMENT
);

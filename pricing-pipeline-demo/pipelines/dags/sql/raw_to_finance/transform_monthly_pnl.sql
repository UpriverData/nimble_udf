-- =====================================================================
-- Transform: RAW.NETSUITE_JOURNAL_ENTRIES -> FINANCE.MONTHLY_PNL
-- Schedule: Daily @ 07:30 UTC (runs after GL accounts + sales summary)
-- =====================================================================
-- Aggregates raw journal entries by posting period and account to
-- produce the monthly P&L summary. Only includes posted entries.

MERGE INTO ENTERPRISE_DW.FINANCE.MONTHLY_PNL AS tgt
USING (
    SELECT
        -- Convert period format: "APR-2025" -> "2025-04"
        CASE
            WHEN LEFT(POSTING_PERIOD, 3) = 'JAN' THEN RIGHT(POSTING_PERIOD, 4) || '-01'
            WHEN LEFT(POSTING_PERIOD, 3) = 'FEB' THEN RIGHT(POSTING_PERIOD, 4) || '-02'
            WHEN LEFT(POSTING_PERIOD, 3) = 'MAR' THEN RIGHT(POSTING_PERIOD, 4) || '-03'
            WHEN LEFT(POSTING_PERIOD, 3) = 'APR' THEN RIGHT(POSTING_PERIOD, 4) || '-04'
            WHEN LEFT(POSTING_PERIOD, 3) = 'MAY' THEN RIGHT(POSTING_PERIOD, 4) || '-05'
            WHEN LEFT(POSTING_PERIOD, 3) = 'JUN' THEN RIGHT(POSTING_PERIOD, 4) || '-06'
            WHEN LEFT(POSTING_PERIOD, 3) = 'JUL' THEN RIGHT(POSTING_PERIOD, 4) || '-07'
            WHEN LEFT(POSTING_PERIOD, 3) = 'AUG' THEN RIGHT(POSTING_PERIOD, 4) || '-08'
            WHEN LEFT(POSTING_PERIOD, 3) = 'SEP' THEN RIGHT(POSTING_PERIOD, 4) || '-09'
            WHEN LEFT(POSTING_PERIOD, 3) = 'OCT' THEN RIGHT(POSTING_PERIOD, 4) || '-10'
            WHEN LEFT(POSTING_PERIOD, 3) = 'NOV' THEN RIGHT(POSTING_PERIOD, 4) || '-11'
            WHEN LEFT(POSTING_PERIOD, 3) = 'DEC' THEN RIGHT(POSTING_PERIOD, 4) || '-12'
        END                                                AS PERIOD,
        'GL-' || ACCOUNT_NUMBER                            AS ACCOUNT_ID,
        -- Net amount: credits are positive (revenue), debits are positive (expense)
        -- For contra accounts (returns), use debit as negative
        SUM(
            CASE
                WHEN ga.ACCOUNT_TYPE = 'CONTRA'
                    THEN -1 * COALESCE(TRY_CAST(NULLIF(DEBIT_AMOUNT,'') AS DECIMAL(14,2)), 0)
                WHEN ga.ACCOUNT_TYPE = 'REVENUE'
                    THEN COALESCE(TRY_CAST(NULLIF(CREDIT_AMOUNT,'') AS DECIMAL(14,2)), 0)
                ELSE COALESCE(TRY_CAST(NULLIF(DEBIT_AMOUNT,'') AS DECIMAL(14,2)), 0)
            END
        )                                                  AS AMOUNT
    FROM ENTERPRISE_DW.RAW.NETSUITE_JOURNAL_ENTRIES je
    JOIN ENTERPRISE_DW.FINANCE.GL_ACCOUNTS ga
        ON ga.ACCOUNT_ID = 'GL-' || je.ACCOUNT_NUMBER
    WHERE je.IS_POSTED = TRUE
    GROUP BY POSTING_PERIOD, ACCOUNT_NUMBER
) AS src
ON tgt.PERIOD = src.PERIOD
   AND tgt.ACCOUNT_ID = src.ACCOUNT_ID
WHEN MATCHED THEN UPDATE SET
    tgt.AMOUNT = src.AMOUNT
WHEN NOT MATCHED THEN INSERT (
    PERIOD, ACCOUNT_ID, AMOUNT
) VALUES (
    src.PERIOD, src.ACCOUNT_ID, src.AMOUNT
);

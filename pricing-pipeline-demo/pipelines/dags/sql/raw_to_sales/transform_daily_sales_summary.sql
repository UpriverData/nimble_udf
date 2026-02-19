-- =====================================================================
-- Transform: RAW.POS_TRANSACTIONS -> SALES.DAILY_SALES_SUMMARY
-- Schedule: Daily @ 07:00 UTC (after POS daily sync + order pipeline)
-- =====================================================================
-- Aggregates POS transactions by date and SKU to produce the daily
-- sales summary. Combines returns information from the RETURN_FLAG.

MERGE INTO ENTERPRISE_DW.SALES.DAILY_SALES_SUMMARY AS tgt
USING (
    SELECT
        TRY_TO_DATE(LEFT(TRANSACTION_TS, 10))             AS SALE_DATE,
        'SKU-' || LPAD(ITEM_NUMBER, 5, '0')               AS SKU_ID,
        SUM(CASE WHEN RETURN_FLAG = 'N'
            THEN TRY_CAST(QTY_SOLD AS INTEGER)
            ELSE 0
        END)                                               AS UNITS_SOLD,
        SUM(CASE WHEN RETURN_FLAG = 'N'
            THEN TRY_CAST(SALE_AMOUNT AS DECIMAL(12,2))
            ELSE 0
        END)                                               AS GROSS_REVENUE,
        SUM(CASE WHEN RETURN_FLAG = 'Y'
            THEN TRY_CAST(QTY_SOLD AS INTEGER)
            ELSE 0
        END)                                               AS RETURNS
    FROM ENTERPRISE_DW.RAW.POS_TRANSACTIONS
    GROUP BY
        TRY_TO_DATE(LEFT(TRANSACTION_TS, 10)),
        ITEM_NUMBER
) AS src
ON tgt.SALE_DATE = src.SALE_DATE
   AND tgt.SKU_ID = src.SKU_ID
WHEN MATCHED THEN UPDATE SET
    tgt.UNITS_SOLD    = src.UNITS_SOLD,
    tgt.GROSS_REVENUE = src.GROSS_REVENUE,
    tgt.RETURNS       = src.RETURNS
WHEN NOT MATCHED THEN INSERT (
    SALE_DATE, SKU_ID, UNITS_SOLD, GROSS_REVENUE, RETURNS
) VALUES (
    src.SALE_DATE, src.SKU_ID, src.UNITS_SOLD, src.GROSS_REVENUE, src.RETURNS
);

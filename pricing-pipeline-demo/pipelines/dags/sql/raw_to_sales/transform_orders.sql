-- =====================================================================
-- Transform: RAW.ERP_SALES_ORDERS -> SALES.ORDERS
-- Schedule: Every 30 min (near-realtime order pipeline)
-- =====================================================================
-- Deduplicates order headers from the line-level ERP feed and maps
-- ERP account IDs to our internal customer IDs.

MERGE INTO ENTERPRISE_DW.SALES.ORDERS AS tgt
USING (
    WITH order_headers AS (
        SELECT
            'ORD-' || ORDER_NUMBER                        AS ORDER_ID,
            -- Map ERP account to internal customer ID
            'CUST-' || REPLACE(CUSTOMER_ACCOUNT, 'ACCT-', '')
                                                          AS CUSTOMER_ID,
            TRY_TO_DATE(LEFT(ORDER_ENTERED_DATE, 10))     AS ORDER_DATE,
            SALES_CHANNEL                                  AS CHANNEL,
            -- Line status rollup: worst status wins at order level
            CASE
                WHEN MIN(LINE_STATUS) OVER (PARTITION BY ORDER_NUMBER) = 'ENTERED'    THEN 'PENDING'
                WHEN MIN(LINE_STATUS) OVER (PARTITION BY ORDER_NUMBER) = 'PROCESSING' THEN 'PROCESSING'
                WHEN MIN(LINE_STATUS) OVER (PARTITION BY ORDER_NUMBER) = 'SHIPPED'    THEN 'SHIPPED'
                WHEN MIN(LINE_STATUS) OVER (PARTITION BY ORDER_NUMBER) = 'CLOSED'     THEN 'DELIVERED'
                ELSE INITCAP(LINE_STATUS)
            END                                            AS STATUS,
            TRY_TO_DATE(LEFT(ACTUAL_SHIP_DATE, 10))        AS SHIP_DATE,
            TRY_CAST(UNIT_SELLING_PRICE AS DECIMAL(12,2))
              * TRY_CAST(ORDERED_QTY AS INTEGER)           AS LINE_AMOUNT
        FROM ENTERPRISE_DW.RAW.ERP_SALES_ORDERS
    )
    SELECT
        ORDER_ID,
        CUSTOMER_ID,
        ORDER_DATE,
        CHANNEL,
        SUM(LINE_AMOUNT)                                   AS TOTAL_AMOUNT,
        MAX(STATUS)                                        AS STATUS,
        MAX(SHIP_DATE)                                     AS SHIP_DATE
    FROM order_headers
    GROUP BY ORDER_ID, CUSTOMER_ID, ORDER_DATE, CHANNEL
) AS src
ON tgt.ORDER_ID = src.ORDER_ID
WHEN MATCHED THEN UPDATE SET
    tgt.CUSTOMER_ID  = src.CUSTOMER_ID,
    tgt.ORDER_DATE   = src.ORDER_DATE,
    tgt.CHANNEL      = src.CHANNEL,
    tgt.TOTAL_AMOUNT = src.TOTAL_AMOUNT,
    tgt.STATUS       = src.STATUS,
    tgt.SHIP_DATE    = src.SHIP_DATE
WHEN NOT MATCHED THEN INSERT (
    ORDER_ID, CUSTOMER_ID, ORDER_DATE, CHANNEL, TOTAL_AMOUNT, STATUS, SHIP_DATE
) VALUES (
    src.ORDER_ID, src.CUSTOMER_ID, src.ORDER_DATE, src.CHANNEL,
    src.TOTAL_AMOUNT, src.STATUS, src.SHIP_DATE
);

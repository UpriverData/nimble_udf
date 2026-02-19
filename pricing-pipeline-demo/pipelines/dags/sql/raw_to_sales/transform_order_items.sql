-- =====================================================================
-- Transform: RAW.ERP_SALES_ORDERS -> SALES.ORDER_ITEMS
-- Schedule: Every 30 min (runs after transform_orders)
-- =====================================================================
-- Extracts individual line items from the ERP order feed and maps
-- them to our internal SKU identifiers.

MERGE INTO ENTERPRISE_DW.SALES.ORDER_ITEMS AS tgt
USING (
    SELECT
        'ORD-' || ORDER_NUMBER                            AS ORDER_ID,
        TRY_CAST(ORDER_LINE AS INTEGER)                   AS LINE_NUMBER,
        'SKU-' || LPAD(ITEM_NUMBER, 5, '0')               AS SKU_ID,
        TRY_CAST(ORDERED_QTY AS INTEGER)                  AS QUANTITY,
        TRY_CAST(UNIT_SELLING_PRICE AS DECIMAL(10,2))     AS UNIT_PRICE,
        TRY_CAST(LINE_DISCOUNT_PCT AS DECIMAL(5,2))       AS DISCOUNT_PCT
    FROM ENTERPRISE_DW.RAW.ERP_SALES_ORDERS
) AS src
ON tgt.ORDER_ID = src.ORDER_ID
   AND tgt.LINE_NUMBER = src.LINE_NUMBER
WHEN MATCHED THEN UPDATE SET
    tgt.SKU_ID       = src.SKU_ID,
    tgt.QUANTITY     = src.QUANTITY,
    tgt.UNIT_PRICE   = src.UNIT_PRICE,
    tgt.DISCOUNT_PCT = src.DISCOUNT_PCT
WHEN NOT MATCHED THEN INSERT (
    ORDER_ID, LINE_NUMBER, SKU_ID, QUANTITY, UNIT_PRICE, DISCOUNT_PCT
) VALUES (
    src.ORDER_ID, src.LINE_NUMBER, src.SKU_ID, src.QUANTITY,
    src.UNIT_PRICE, src.DISCOUNT_PCT
);

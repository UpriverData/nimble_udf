-- =====================================================================
-- Transform: RAW.ERP_WAREHOUSE_INVENTORY -> INVENTORY.WAREHOUSE_STOCK
-- Schedule: Hourly (after WMS snapshot sync)
-- =====================================================================
-- Takes the latest inventory snapshot per warehouse+SKU combination
-- and writes it to the curated stock table.

MERGE INTO ENTERPRISE_DW.INVENTORY.WAREHOUSE_STOCK AS tgt
USING (
    SELECT
        'WH-' || FACILITY_CODE                          AS WAREHOUSE_ID,
        'SKU-' || LPAD(ITEM_NUMBER, 5, '0')             AS SKU_ID,
        TRY_CAST(ON_HAND_QTY AS INTEGER)                AS QUANTITY_ON_HAND,
        TRY_CAST(REORDER_LEVEL AS INTEGER)              AS REORDER_POINT,
        TRY_TO_DATE(LEFT(LAST_CYCLE_COUNT, 10))         AS LAST_COUNTED
    FROM ENTERPRISE_DW.RAW.ERP_WAREHOUSE_INVENTORY
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY FACILITY_CODE, ITEM_NUMBER
        ORDER BY TRY_TO_TIMESTAMP_NTZ(SNAPSHOT_TS) DESC
    ) = 1
) AS src
ON tgt.WAREHOUSE_ID = src.WAREHOUSE_ID
   AND tgt.SKU_ID = src.SKU_ID
WHEN MATCHED THEN UPDATE SET
    tgt.QUANTITY_ON_HAND = src.QUANTITY_ON_HAND,
    tgt.REORDER_POINT    = src.REORDER_POINT,
    tgt.LAST_COUNTED     = src.LAST_COUNTED
WHEN NOT MATCHED THEN INSERT (
    WAREHOUSE_ID, SKU_ID, QUANTITY_ON_HAND, REORDER_POINT, LAST_COUNTED
) VALUES (
    src.WAREHOUSE_ID, src.SKU_ID, src.QUANTITY_ON_HAND, src.REORDER_POINT, src.LAST_COUNTED
);

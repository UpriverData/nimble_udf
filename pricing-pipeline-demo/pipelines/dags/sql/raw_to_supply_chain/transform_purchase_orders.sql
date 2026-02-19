-- =====================================================================
-- Transform: RAW.SAP_PURCHASE_ORDERS -> SUPPLY_CHAIN.PURCHASE_ORDERS
-- Schedule: Daily @ 06:00 UTC (runs after transform_suppliers)
-- =====================================================================
-- Transforms raw SAP purchase order data into the clean PO table.
-- Joins on vendor number to resolve our internal supplier IDs.

MERGE INTO ENTERPRISE_DW.SUPPLY_CHAIN.PURCHASE_ORDERS AS tgt
USING (
    SELECT
        'PO-' || RIGHT(spo.PO_NUMBER, 4)                   AS PO_ID,
        'SUP-' || RIGHT(spo.VENDOR_NUMBER, 3)               AS SUPPLIER_ID,
        'SKU-' || LPAD(spo.MATERIAL_NUMBER, 5, '0')         AS SKU_ID,
        TRY_CAST(spo.PO_QTY AS INTEGER)                    AS QUANTITY,
        TRY_CAST(spo.NET_PRICE AS DECIMAL(10,2))           AS UNIT_COST,
        TRY_TO_DATE(spo.DOC_DATE)                          AS ORDER_DATE,
        TRY_TO_DATE(spo.DELIVERY_DATE)                     AS EXPECTED_DATE,
        -- Map SAP PO status to our internal status
        CASE
            WHEN spo.PO_STATUS = 'FULLY_RECEIVED' THEN 'RECEIVED'
            WHEN spo.PO_STATUS = 'IN_TRANSIT'     THEN 'IN_TRANSIT'
            WHEN spo.PO_STATUS = 'ORDERED'        THEN 'ORDERED'
            ELSE spo.PO_STATUS
        END                                                 AS STATUS
    FROM ENTERPRISE_DW.RAW.SAP_PURCHASE_ORDERS spo
    WHERE spo.PO_LINE = '10'  -- Only first line per PO in this dataset
) AS src
ON tgt.PO_ID = src.PO_ID
WHEN MATCHED THEN UPDATE SET
    tgt.SUPPLIER_ID   = src.SUPPLIER_ID,
    tgt.SKU_ID        = src.SKU_ID,
    tgt.QUANTITY      = src.QUANTITY,
    tgt.UNIT_COST     = src.UNIT_COST,
    tgt.ORDER_DATE    = src.ORDER_DATE,
    tgt.EXPECTED_DATE = src.EXPECTED_DATE,
    tgt.STATUS        = src.STATUS
WHEN NOT MATCHED THEN INSERT (
    PO_ID, SUPPLIER_ID, SKU_ID, QUANTITY, UNIT_COST, ORDER_DATE, EXPECTED_DATE, STATUS
) VALUES (
    src.PO_ID, src.SUPPLIER_ID, src.SKU_ID, src.QUANTITY,
    src.UNIT_COST, src.ORDER_DATE, src.EXPECTED_DATE, src.STATUS
);

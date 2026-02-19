-- =====================================================================
-- Transform: RAW.ERP_PRODUCTS -> INVENTORY.SKUS
-- Schedule: Daily @ 06:00 UTC (after Fivetran ERP sync completes)
-- =====================================================================

MERGE INTO ENTERPRISE_DW.INVENTORY.SKUS AS tgt
USING (
    SELECT
        'SKU-' || LPAD(ITEM_NUMBER, 5, '0')                  AS SKU_ID,
        ITEM_DESCRIPTION                                       AS PRODUCT_NAME,
        -- Normalize manufacturer name to brand
        CASE
            WHEN MANUFACTURER ILIKE '%Dyson%'       THEN 'Dyson'
            WHEN MANUFACTURER ILIKE '%Whirlpool%'   THEN 'KitchenAid'
            WHEN MANUFACTURER ILIKE '%Sony%'        THEN 'Sony'
            WHEN MANUFACTURER ILIKE '%DeLonghi%'    THEN 'Nespresso'
            WHEN MANUFACTURER ILIKE '%iRobot%'      THEN 'iRobot'
            WHEN MANUFACTURER ILIKE '%Bose%'        THEN 'Bose'
            WHEN MANUFACTURER ILIKE '%Instant%'     THEN 'Instant Pot'
            WHEN MANUFACTURER ILIKE '%Samsung%'     THEN 'Samsung'
            WHEN MANUFACTURER ILIKE '%Vitamix%'     THEN 'Vitamix'
            WHEN MANUFACTURER ILIKE '%Apple%'       THEN 'Apple'
            ELSE SPLIT_PART(MANUFACTURER, ' ', 1)
        END                                                    AS BRAND,
        UPC_CODE                                               AS UPC,
        TRY_CAST(LIST_PRICE AS DECIMAL(10,2))                 AS MSRP,
        -- Map ERP item types to product categories
        CASE
            WHEN ITEM_SUBTYPE IN ('VACUUM','KITCHEN','PERSONAL') THEN 'Home Appliances'
            WHEN ITEM_SUBTYPE IN ('AUDIO','TELEVISION')          THEN 'Electronics'
            ELSE 'Other'
        END                                                    AS CATEGORY,
        CASE
            WHEN ITEM_SUBTYPE = 'VACUUM'     THEN 'Vacuums'
            WHEN ITEM_SUBTYPE = 'KITCHEN'    THEN 'Kitchen'
            WHEN ITEM_SUBTYPE = 'AUDIO'      THEN 'Audio'
            WHEN ITEM_SUBTYPE = 'TELEVISION' THEN 'Televisions'
            WHEN ITEM_SUBTYPE = 'PERSONAL'   THEN 'Personal Care'
            ELSE ITEM_SUBTYPE
        END                                                    AS SUBCATEGORY,
        CASE
            WHEN WEIGHT_UOM = 'LBS' THEN TRY_CAST(WEIGHT AS DECIMAL(8,2))
            WHEN WEIGHT_UOM = 'KG'  THEN TRY_CAST(WEIGHT AS DECIMAL(8,2)) * 2.20462
            ELSE TRY_CAST(WEIGHT AS DECIMAL(8,2))
        END                                                    AS WEIGHT_LBS,
        CASE
            WHEN ITEM_STATUS = 'ACTIVE'   THEN 'ACTIVE'
            WHEN ITEM_STATUS = 'INACTIVE' THEN 'INACTIVE'
            ELSE 'UNKNOWN'
        END                                                    AS STATUS,
        TRY_TO_TIMESTAMP_NTZ(CREATED_DATE)                    AS CREATED_AT
    FROM ENTERPRISE_DW.RAW.ERP_PRODUCTS
    WHERE ITEM_TYPE = 'FINISHED_GOOD'
      AND ITEM_STATUS = 'ACTIVE'
) AS src
ON tgt.SKU_ID = src.SKU_ID
WHEN MATCHED THEN UPDATE SET
    tgt.PRODUCT_NAME = src.PRODUCT_NAME,
    tgt.BRAND        = src.BRAND,
    tgt.UPC          = src.UPC,
    tgt.MSRP         = src.MSRP,
    tgt.CATEGORY     = src.CATEGORY,
    tgt.SUBCATEGORY  = src.SUBCATEGORY,
    tgt.WEIGHT_LBS   = src.WEIGHT_LBS,
    tgt.STATUS       = src.STATUS
WHEN NOT MATCHED THEN INSERT (
    SKU_ID, PRODUCT_NAME, BRAND, UPC, MSRP,
    CATEGORY, SUBCATEGORY, WEIGHT_LBS, STATUS, CREATED_AT
) VALUES (
    src.SKU_ID, src.PRODUCT_NAME, src.BRAND, src.UPC, src.MSRP,
    src.CATEGORY, src.SUBCATEGORY, src.WEIGHT_LBS, src.STATUS, src.CREATED_AT
);

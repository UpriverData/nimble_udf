-- =====================================================================
-- Transform: RAW.ERP_PRODUCTS -> INVENTORY.PRODUCT_CATEGORIES
-- Schedule: Daily @ 06:00 UTC (runs after transform_skus)
-- =====================================================================
-- Derives the category taxonomy from product data. Since the ERP
-- doesn't maintain a separate category master, we extract distinct
-- categories from the product catalog.

MERGE INTO ENTERPRISE_DW.INVENTORY.PRODUCT_CATEGORIES AS tgt
USING (
    WITH raw_categories AS (
        SELECT DISTINCT
            ITEM_SUBTYPE,
            CASE
                WHEN ITEM_SUBTYPE IN ('VACUUM','KITCHEN','PERSONAL') THEN 'Home Appliances'
                WHEN ITEM_SUBTYPE IN ('AUDIO','TELEVISION')          THEN 'Electronics'
                ELSE 'Other'
            END AS parent_category,
            CASE
                WHEN ITEM_SUBTYPE IN ('VACUUM','KITCHEN','PERSONAL') THEN 'Home & Living'
                WHEN ITEM_SUBTYPE IN ('AUDIO','TELEVISION')          THEN 'Technology'
                ELSE 'General'
            END AS department
        FROM ENTERPRISE_DW.RAW.ERP_PRODUCTS
        WHERE ITEM_TYPE = 'FINISHED_GOOD'
          AND ITEM_STATUS = 'ACTIVE'
    ),
    -- Build parent-level categories
    parents AS (
        SELECT DISTINCT
            parent_category AS CATEGORY_NAME,
            NULL            AS PARENT_CATEGORY,
            department      AS DEPARTMENT
        FROM raw_categories
    ),
    -- Build child-level categories (subcategories)
    children AS (
        SELECT DISTINCT
            CASE
                WHEN ITEM_SUBTYPE = 'VACUUM'     THEN 'Vacuums'
                WHEN ITEM_SUBTYPE = 'KITCHEN'    THEN 'Kitchen'
                WHEN ITEM_SUBTYPE = 'AUDIO'      THEN 'Audio'
                WHEN ITEM_SUBTYPE = 'TELEVISION' THEN 'Televisions'
                WHEN ITEM_SUBTYPE = 'PERSONAL'   THEN 'Personal Care'
                ELSE ITEM_SUBTYPE
            END             AS CATEGORY_NAME,
            parent_category AS PARENT_CATEGORY,
            department      AS DEPARTMENT
        FROM raw_categories
    ),
    all_cats AS (
        SELECT * FROM parents
        UNION
        SELECT * FROM children
    )
    SELECT
        'CAT-' || LPAD(ROW_NUMBER() OVER (ORDER BY PARENT_CATEGORY NULLS FIRST, CATEGORY_NAME)::VARCHAR, 3, '0') AS CATEGORY_ID,
        CATEGORY_NAME,
        PARENT_CATEGORY,
        DEPARTMENT
    FROM all_cats
) AS src
ON tgt.CATEGORY_NAME = src.CATEGORY_NAME
WHEN MATCHED THEN UPDATE SET
    tgt.PARENT_CATEGORY = src.PARENT_CATEGORY,
    tgt.DEPARTMENT      = src.DEPARTMENT
WHEN NOT MATCHED THEN INSERT (
    CATEGORY_ID, CATEGORY_NAME, PARENT_CATEGORY, DEPARTMENT
) VALUES (
    src.CATEGORY_ID, src.CATEGORY_NAME, src.PARENT_CATEGORY, src.DEPARTMENT
);

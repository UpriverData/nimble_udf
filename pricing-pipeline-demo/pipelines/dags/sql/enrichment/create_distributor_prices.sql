-- =============================================================================
-- Create the DISTRIBUTOR_PRICES output table
-- =============================================================================

USE DATABASE ENTERPRISE_DW;
USE SCHEMA INVENTORY;

CREATE TABLE IF NOT EXISTS DISTRIBUTOR_PRICES (
    SKU_ID              VARCHAR(20),
    PRODUCT_NAME        VARCHAR(200),
    BRAND               VARCHAR(100),
    UPC                 VARCHAR(20),
    MSRP                DECIMAL(10,2),
    DISTRIBUTOR_ID      VARCHAR(20),
    DISTRIBUTOR_NAME    VARCHAR(100),
    WEBSITE_DOMAIN      VARCHAR(200),
    LISTED_PRICE        DECIMAL(10,2),
    PRICE_DEVIATION_PCT DECIMAL(8,4),
    DISCOUNT            DECIMAL(5,2),
    LISTING_URL         VARCHAR(500),
    ENRICHED_AT         TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
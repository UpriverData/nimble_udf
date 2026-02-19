-- =============================================================================
-- Distributor Price Enrichment
-- =============================================================================

-- Cross-joins every active SKU with every active distributor, then calls
-- nimble_enrich() UDTF to look up real-time pricing on each retailer site.
-- Full-refresh pattern: truncate then insert.
-- =============================================================================

USE DATABASE ENTERPRISE_DW;
USE SCHEMA INVENTORY;

TRUNCATE TABLE DISTRIBUTOR_PRICES;

INSERT INTO DISTRIBUTOR_PRICES (
    SKU_ID, PRODUCT_NAME, BRAND, UPC, MSRP,
    DISTRIBUTOR_ID, DISTRIBUTOR_NAME, WEBSITE_DOMAIN,
    LISTED_PRICE, PRICE_DEVIATION_PCT, DISCOUNT, LISTING_URL
)
SELECT
    e.input:sku_id::STRING              AS SKU_ID,
    e.input:product_name::STRING        AS PRODUCT_NAME,
    e.input:brand::STRING               AS BRAND,
    e.input:upc::STRING                 AS UPC,
    e.input:msrp::DECIMAL(10,2)         AS MSRP,
    e.input:distributor_id::STRING      AS DISTRIBUTOR_ID,
    e.input:distributor_name::STRING    AS DISTRIBUTOR_NAME,
    e.input:website_domain::STRING      AS WEBSITE_DOMAIN,

    TRY_CAST(e.enriched:listed_price::STRING AS DECIMAL(10,2))  AS LISTED_PRICE,
    CASE
        WHEN TRY_CAST(e.enriched:listed_price::STRING AS DECIMAL(10,2)) IS NOT NULL
        THEN ROUND(
            (TRY_CAST(e.enriched:listed_price::STRING AS DECIMAL(10,2)) - e.input:msrp::DECIMAL(10,2))
            / e.input:msrp::DECIMAL(10,2),
            4
        )
        ELSE NULL
    END                                                         AS PRICE_DEVIATION_PCT,
    TRY_CAST(e.enriched:discount::STRING AS DECIMAL(5,2))       AS DISCOUNT,
    e.enriched:listing_url::STRING                               AS LISTING_URL

FROM (
    SELECT
        s.SKU_ID,
        s.PRODUCT_NAME,
        s.BRAND,
        s.UPC,
        s.MSRP,
        d.DISTRIBUTOR_ID,
        d.DISTRIBUTOR_NAME,
        d.WEBSITE_DOMAIN
    FROM ENTERPRISE_DW.INVENTORY.SKUS s
    CROSS JOIN ENTERPRISE_DW.INVENTORY.DISTRIBUTORS d
    WHERE s.STATUS = 'ACTIVE'
      AND d.STATUS = 'ACTIVE'
) base,
TABLE(NIMBLE_INTEGRATION.ENRICHMENT.nimble_enrich(
    TO_JSON(OBJECT_CONSTRUCT(
        'sku_id',           base.SKU_ID,
        'product_name',     base.PRODUCT_NAME,
        'brand',            base.BRAND,
        'upc',              base.UPC,
        'msrp',             base.MSRP,
        'distributor_id',   base.DISTRIBUTOR_ID,
        'distributor_name', base.DISTRIBUTOR_NAME,
        'website_domain',   base.WEBSITE_DOMAIN
    )),
    ARRAY_CONSTRUCT('listed_price', 'discount', 'listing_url'),
    'You are a retail price research agent. Your job is to find the current selling price of a product on a specific retailer website.

You will receive a product name, brand, UPC, and a target retailer website domain.

**Instructions:**
1. Use nimble_web_search to search for the product on the specified retailer site. Use a query like: site:<website_domain> "<product_name>" OR "<upc>"
2. If search results include a price, extract it. If not, use nimble_extract_content on the most relevant product URL to get the price from the page.
3. Return the results as JSON with these exact keys:
   - listed_price: the current selling price as a number (e.g. 699.99). Use null if not found.
   - discount: the discount percentage as a decimal (e.g. 0.10 for 10% off). Use 0 if no discount, null if not found.
   - listing_url: the full URL of the product page. Use null if not found.

Be precise. Only return prices from the specified retailer domain. Do not guess or hallucinate prices.'
) OVER (PARTITION BY 1)) e;
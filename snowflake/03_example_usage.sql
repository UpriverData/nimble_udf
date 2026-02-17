-- =============================================================================
-- Nimble Enrichment UDTF - Example Usage
-- =============================================================================
-- These examples show different ways to use the nimble_enrich() UDTF.
-- Make sure you've run 01_setup.sql and 02_create_udtf.sql first.
-- =============================================================================


-- =============================================================================
-- Example 1: Company enrichment
-- =============================================================================
-- Find CEO, founding year, and headquarters for a list of companies.

CREATE OR REPLACE TEMPORARY TABLE sample_companies AS
SELECT * FROM (VALUES
    ('Anthropic',   'anthropic.com'),
    ('Databricks',  'databricks.com'),
    ('Stripe',      'stripe.com'),
    ('Figma',       'figma.com')
) AS t(company_name, website);

SELECT
    e.input:company_name::STRING     AS company_name,
    e.enriched:ceo::STRING           AS ceo,
    e.enriched:year_founded::STRING  AS year_founded,
    e.enriched:headquarters::STRING  AS headquarters,
    e.enriched:funding::STRING       AS total_funding
FROM sample_companies t,
     TABLE(NIMBLE_INTEGRATION.ENRICHMENT.nimble_enrich(
         TO_JSON(OBJECT_CONSTRUCT('company_name', t.company_name, 'website', t.website)),
         ARRAY_CONSTRUCT('ceo', 'year_founded', 'headquarters', 'funding')
     ) OVER (PARTITION BY 1)) e;


-- =============================================================================
-- Example 2: Product price lookup
-- =============================================================================
-- Find the current price of a product on a specific retailer website.

SELECT
    e.input:product_name::STRING        AS product_name,
    e.input:website_domain::STRING      AS retailer,
    e.enriched:listed_price::STRING     AS price,
    e.enriched:discount::STRING         AS discount,
    e.enriched:listing_url::STRING      AS url
FROM (
    SELECT 'Sony WH-1000XM5' AS product_name, 'amazon.com' AS website_domain
) t,
TABLE(NIMBLE_INTEGRATION.ENRICHMENT.nimble_enrich(
    TO_JSON(OBJECT_CONSTRUCT(
        'product_name', t.product_name,
        'website_domain', t.website_domain
    )),
    ARRAY_CONSTRUCT('listed_price', 'discount', 'listing_url'),
    'You are a price research agent. Search for the product on the specified retailer website.
Return JSON with:
- listed_price: current selling price as a number (e.g. 299.99). Use null if not found.
- discount: discount percentage as a decimal (e.g. 0.10 for 10% off). Use 0 if none, null if not found.
- listing_url: full URL of the product page. Use null if not found.
Only return prices from the specified domain.'
) OVER (PARTITION BY 1)) e;


-- =============================================================================
-- Example 3: Save enrichment results to a new table
-- =============================================================================
-- Enrich and materialize in a single statement.

CREATE OR REPLACE TABLE my_enriched_companies AS
SELECT
    e.input:company_name::STRING     AS company_name,
    e.input:website::STRING          AS website,
    e.enriched:ceo::STRING           AS ceo,
    e.enriched:year_founded::STRING  AS year_founded,
    e.enriched:headquarters::STRING  AS headquarters,
    e.enriched:funding::STRING       AS total_funding,
    CURRENT_TIMESTAMP()              AS enriched_at
FROM sample_companies t,
     TABLE(NIMBLE_INTEGRATION.ENRICHMENT.nimble_enrich(
         TO_JSON(OBJECT_CONSTRUCT('company_name', t.company_name, 'website', t.website)),
         ARRAY_CONSTRUCT('ceo', 'year_founded', 'headquarters', 'funding')
     ) OVER (PARTITION BY 1)) e;

SELECT * FROM my_enriched_companies;


-- =============================================================================
-- Example 4: Bulk product enrichment across multiple retailers
-- =============================================================================
-- Cross-join products with retailers and look up prices for every combination.

CREATE OR REPLACE TEMPORARY TABLE sample_products AS
SELECT * FROM (VALUES
    ('Dyson V15 Detect Cordless Vacuum', 'Dyson', '885609024776', 749.99),
    ('Apple AirPods Pro 2nd Generation', 'Apple', '194253945956', 249.99)
) AS t(product_name, brand, upc, msrp);

CREATE OR REPLACE TEMPORARY TABLE sample_retailers AS
SELECT * FROM (VALUES
    ('bestbuy.com'),
    ('amazon.com'),
    ('walmart.com')
) AS t(website_domain);

SELECT
    e.input:product_name::STRING     AS product,
    e.input:website_domain::STRING   AS retailer,
    e.input:msrp::STRING             AS msrp,
    e.enriched:listed_price::STRING  AS listed_price,
    e.enriched:discount::STRING      AS discount,
    e.enriched:listing_url::STRING   AS listing_url
FROM (
    SELECT p.*, r.website_domain
    FROM sample_products p
    CROSS JOIN sample_retailers r
) t,
TABLE(NIMBLE_INTEGRATION.ENRICHMENT.nimble_enrich(
    TO_JSON(OBJECT_CONSTRUCT(
        'product_name', t.product_name,
        'brand', t.brand,
        'upc', t.upc,
        'msrp', t.msrp,
        'website_domain', t.website_domain
    )),
    ARRAY_CONSTRUCT('listed_price', 'discount', 'listing_url'),
    'You are a price research agent. Search for the product on the specified retailer site using: site:<website_domain> "<product_name>" OR "<upc>"
Return JSON with:
- listed_price: current selling price as a number. Use null if not found.
- discount: discount percentage as a decimal. Use 0 if none, null if not found.
- listing_url: full product page URL. Use null if not found.
Be precise. Only return prices from the specified domain.'
) OVER (PARTITION BY 1)) e;

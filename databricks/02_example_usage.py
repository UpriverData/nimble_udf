"""
Nimble Enrichment UDF - Example Usage

Examples showing different ways to call the nimble_enrich() Unity Catalog UDF from SQL.
No secrets needed in the call — they are injected by the wrapper function.

Prerequisite: Run 01_setup_and_udf first to register the UDF in Unity Catalog.
"""

# --- Configuration ---

CATALOG = "main"
SCHEMA = "default"

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")
print(f"UDF: {CATALOG}.{SCHEMA}.nimble_enrich")

# --- Example 1: Single-row enrichment ---

result = spark.sql(f"""
SELECT nimble_enrich(
    '{{"company_name": "Anthropic", "website": "anthropic.com"}}',
    '["ceo", "year_founded", "headquarters", "funding"]'
) AS enriched
""")
display(result)

# --- Example 2: Enrich a table ---

# Create a sample companies table
spark.sql("""
CREATE OR REPLACE TEMP VIEW companies AS
SELECT * FROM VALUES
    ('Anthropic',  'anthropic.com'),
    ('Stripe',     'stripe.com'),
    ('Databricks', 'databricks.com')
AS (company_name, website)
""")

# Enrich every row — the UDF is called once per row
enriched = spark.sql("""
SELECT
    company_name,
    website,
    nimble_enrich(
        to_json(named_struct('company_name', company_name, 'website', website)),
        '["ceo", "year_founded", "headquarters", "funding"]'
    ) AS enriched_json
FROM companies
""")
display(enriched)

# --- Example 3: Custom prompt — price lookup ---

PRICE_PROMPT = "You are a price research agent. Search for the product on the specified retailer website. Return JSON with: listed_price (number), discount (decimal, 0 if none), listing_url (full URL). Only return prices from the specified domain."

enriched_prices = spark.sql(f"""
SELECT nimble_enrich(
    '{{"product_name": "Sony WH-1000XM5 Wireless Headphones", "brand": "Sony", "website_domain": "amazon.com"}}',
    '["listed_price", "discount", "listing_url"]',
    '{PRICE_PROMPT}'
) AS enriched
""")
display(enriched_prices)

# --- Example 4: Parse enriched JSON into columns & save to Delta table ---

spark.sql("""
CREATE OR REPLACE TABLE enriched_companies AS
WITH enriched AS (
    SELECT
        company_name,
        website,
        nimble_enrich(
            to_json(named_struct('company_name', company_name, 'website', website)),
            '["ceo", "year_founded", "headquarters", "funding"]'
        ) AS enriched_json
    FROM companies
)
SELECT
    company_name,
    website,
    get_json_object(enriched_json, '$.ceo')          AS ceo,
    get_json_object(enriched_json, '$.year_founded')  AS year_founded,
    get_json_object(enriched_json, '$.headquarters')  AS headquarters,
    get_json_object(enriched_json, '$.funding')       AS funding,
    current_timestamp()                                AS enriched_at
FROM enriched
""")

display(spark.sql("SELECT * FROM enriched_companies"))

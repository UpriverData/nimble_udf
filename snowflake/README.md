# Nimble Enrichment UDTF for Snowflake

A Snowflake User-Defined Table Function (UDTF) that uses a LangChain agent with [Nimble's](https://nimbleway.com) Search and Extract tools to enrich any table with live web data.

## Architecture

```
┌───────────────┐    ┌────────────────────────────────────────────┐
│  Your SQL     │    │           Snowflake UDTF                   │
│  Query        │───▶│  ┌──────────────────────────────────────┐  │
│               │    │  │  Python Handler (NimbleEnrichHandler)│  │
│  SELECT ...   │    │  │                                      │  │
│  FROM t,      │    │  │  process() → collects rows           │  │
│  TABLE(       │    │  │  end_partition() → runs agent        │  │
│    nimble_    │    │  │    ├─ NimbleSearchTool               │  │
│    enrich()   │    │  │    ├─ NimbleExtractTool              │  │
│  )            │    │  │    └─ Claude (Anthropic API)         │  │
│               │◀───│  │  yields (input, enriched) per row    │  │
└───────────────┘    │  └──────────────────────────────────────┘  │
                     └────────────────────────────────────────────┘
```

## Prerequisites

- Snowflake account with `ACCOUNTADMIN` role
- [Nimble API key](https://app.nimbleway.com/account_settings/api_keys)
- [Anthropic API key](https://console.anthropic.com/)

## Setup

### Step 1: Configure infrastructure

Open [`01_setup.sql`](01_setup.sql) and replace the API key placeholders:

```sql
-- Replace these with your actual keys:
SECRET_STRING = 'YOUR_NIMBLE_API_KEY'
SECRET_STRING = 'YOUR_ANTHROPIC_API_KEY'
```

Then run the script. It creates:
- Network rule allowing egress to `nimble-retriever.webit.live` and `api.anthropic.com`
- Two secrets (Nimble API key + Anthropic API key)
- External access integration combining both
- `NIMBLE_DEVELOPER` and `NIMBLE_USER` roles

### Step 2: Create the UDTF

Run [`02_create_udtf.sql`](02_create_udtf.sql) to create the `nimble_enrich()` function.

### Step 3: Enrich your data

See [`03_example_usage.sql`](03_example_usage.sql) for ready-to-run examples.

## Usage

### Basic: Enrich companies

```sql
SELECT
    e.input:company_name::STRING  AS company_name,
    e.enriched:ceo::STRING        AS ceo,
    e.enriched:founded::STRING    AS year_founded,
    e.enriched:headquarters::STRING AS headquarters
FROM my_companies t,
     TABLE(NIMBLE_INTEGRATION.ENRICHMENT.nimble_enrich(
         TO_JSON(OBJECT_CONSTRUCT('company_name', t.name, 'website', t.domain)),
         ARRAY_CONSTRUCT('ceo', 'founded', 'headquarters')
     ) OVER (PARTITION BY 1)) e;
```

### With custom prompt: Price lookup

```sql
SELECT
    e.input:product_name::STRING    AS product,
    e.enriched:listed_price::STRING AS price,
    e.enriched:listing_url::STRING  AS url
FROM products t,
     TABLE(NIMBLE_INTEGRATION.ENRICHMENT.nimble_enrich(
         TO_JSON(OBJECT_CONSTRUCT(
             'product_name', t.name,
             'upc', t.upc,
             'website_domain', 'amazon.com'
         )),
         ARRAY_CONSTRUCT('listed_price', 'discount', 'listing_url'),
         'You are a price research agent. Search for the product on the given website and return the current price.'
     ) OVER (PARTITION BY 1)) e;
```

### Function Signature

```sql
nimble_enrich(
    input_json     VARCHAR,   -- JSON object with entity attributes
    output_columns ARRAY,     -- Array of field names to enrich
    custom_prompt  VARCHAR    -- (Optional) Custom system prompt
)
RETURNS TABLE (
    input    VARIANT,  -- Original input passed through
    enriched VARIANT   -- JSON object with enriched fields
)
```

### Important: `PARTITION BY 1`

Always use `OVER (PARTITION BY 1)` in your query. This groups all rows into a single partition so the agent is initialized once and processes rows sequentially — minimizing API overhead.

## Files

| File | Description |
|---|---|
| `01_setup.sql` | Network rules, secrets, external access integration, roles |
| `02_create_udtf.sql` | The `nimble_enrich()` UDTF with embedded Python agent |
| `03_example_usage.sql` | Example enrichment queries |

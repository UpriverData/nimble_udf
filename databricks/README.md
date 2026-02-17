# Nimble Enrichment UDF for Databricks

A **Unity Catalog governed** Python UDF that uses a LangChain agent with [Nimble's](https://nimbleway.com) Search and Extract tools to enrich any table with live web data. Registered as a permanent function in Unity Catalog and callable from **any SQL query, notebook, or dashboard**.

## Architecture

```
┌───────────────────┐    ┌──────────────────────────────────────┐
│  Any SQL client   │    │  catalog.schema.nimble_enrich()       │
│                   │    │  catalog.schema.nimble_enrich_anthropic()
│  SQL Warehouse    │───▶│                                      │
│  Notebook         │    │  LangChain Agent                     │
│  Dashboard        │    │    ├─ NimbleSearchTool               │
│  Scheduled Job    │◀───│    ├─ NimbleExtractTool              │
│                   │    │    └─ ChatDatabricks or ChatAnthropic│
│  Returns JSON     │    │                                      │
└───────────────────┘    │  Unity Catalog governed              │
                         └──────────────────────────────────────┘
```

## Prerequisites

- Databricks workspace with Unity Catalog
- Serverless or Pro SQL warehouse (or DBR 16.3+ cluster)
- [Nimble API key](https://app.nimbleway.com/account_settings/api_keys)
- **Either:** Databricks Model Serving endpoint with Claude (e.g., `databricks-claude-sonnet-4-5`), **or** [Anthropic API key](https://console.anthropic.com/settings/keys)

## Setup

### Step 1: Store secrets

```bash
# Create a secret scope (one-time)
databricks secrets create-scope nimble

# Store the Nimble API key
databricks secrets put-secret nimble nimble_api_key

# For ChatDatabricks (Model Serving):
databricks secrets put-secret nimble databricks_token

# For ChatAnthropic (Anthropic API):
databricks secrets put-secret nimble anthropic_api_key
```

### Step 2: Create the UDF

Import [`01_setup_and_udf.ipynb`](01_setup_and_udf.ipynb) into your Databricks workspace and run it. It creates three functions:

1. **`_nimble_enrich_internal`** — Python UDF with the agent logic (accepts credentials + provider as parameters)
2. **`nimble_enrich`** — SQL wrapper using **ChatDatabricks** (Model Serving). Injects `secret()` and workspace host automatically.
3. **`nimble_enrich_anthropic`** — SQL wrapper using **ChatAnthropic** (Anthropic API). Injects `secret()` automatically.

### Step 3: Enrich your data

See [`02_example_usage.ipynb`](02_example_usage.ipynb) for ready-to-run examples.

## Usage

Both public wrappers have the same clean 3-parameter signature — no secrets needed in the call:

```sql
-- Using ChatDatabricks (Model Serving):
nimble_enrich(input_json STRING, output_columns STRING [, custom_prompt STRING])

-- Using ChatAnthropic (Anthropic API):
nimble_enrich_anthropic(input_json STRING, output_columns STRING [, custom_prompt STRING])
```

### Single row

```sql
SELECT nimble_enrich(
    '{"company_name": "Anthropic", "website": "anthropic.com"}',
    '["ceo", "year_founded", "headquarters"]'
) AS enriched;
```

### Enrich a table

```sql
SELECT
    company_name,
    website,
    nimble_enrich(
        to_json(named_struct('company_name', company_name, 'website', website)),
        '["ceo", "year_founded", "headquarters", "funding"]'
    ) AS enriched_json
FROM my_companies;
```

### With custom prompt

```sql
SELECT nimble_enrich(
    '{"product_name": "Dyson V15", "website_domain": "bestbuy.com"}',
    '["listed_price", "discount", "listing_url"]',
    'You are a price research agent. Find the current selling price on the specified retailer website.'
) AS enriched;
```

### Parse enriched JSON into columns

```sql
SELECT
    company_name,
    get_json_object(enriched_json, '$.ceo')          AS ceo,
    get_json_object(enriched_json, '$.year_founded')  AS year_founded,
    get_json_object(enriched_json, '$.headquarters')  AS headquarters
FROM enriched_companies;
```

## Files

Each notebook is provided in two formats: `.ipynb` (Jupyter/GitHub-renderable) and `.py` (plain Python).

| File | Description |
|---|---|
| `01_setup_and_udf.ipynb` | Notebook: creates all three UC functions, verifies, runs a test |
| `02_example_usage.ipynb` | Notebook: example enrichment patterns |
| `01_setup_and_udf.py` | Same as above in plain Python |
| `02_example_usage.py` | Same as above in plain Python |

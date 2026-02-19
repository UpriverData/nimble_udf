# Pricing Pipeline Demo

This folder contains the **exact code** shown in the Nimble UDF demo video. It demonstrates how to build a continuously updating dataset that shows the current price each distributor is charging for your products.

![Upriver + Nimble UDF demo screenshot](images/upriver_nimble_udf.png)

## Watch the Demo

<video src="images/Nimble UDF.mp4" controls width="800">
  Your browser does not support the video tag. [Download the video](images/Nimble%20UDF.mp4) instead.
</video>

## What's Included

The demo builds a pricing enrichment pipeline that:

1. **Cross-joins** SKUs × Distributors from your warehouse
2. **Calls** the `nimble_enrich()` UDTF to fetch live pricing from each retailer's website
3. **Produces** an enriched `DISTRIBUTOR_PRICES` table with real-time prices, discounts, and listing URLs

### Key Files

| File | Description |
|------|-------------|
| `pipelines/dags/enrichment_pipeline.py` | Airflow DAG that orchestrates the enrichment pipeline |
| `pipelines/dags/sql/enrichment/create_nimble_udtf.sql` | The Nimble UDTF definition (Python + LangChain agent) |
| `pipelines/dags/sql/enrichment/setup_nimble_integration.sql` | Network rules, secrets, and external access integration |
| `pipelines/dags/sql/enrichment/create_distributor_prices.sql` | Output table schema |
| `pipelines/dags/sql/enrichment/enrich_distributor_prices.sql` | The enrichment query |

### Pipeline Flow

```
INVENTORY.SKUS + INVENTORY.DISTRIBUTORS → nimble_enrich() → INVENTORY.DISTRIBUTOR_PRICES
```

The DAG also includes validation tasks (row count, null checks, price range checks) to ensure data quality.

"""
DAG: inventory_pipeline
Schedule: Daily @ 06:00 UTC
Owner: data-engineering

Transforms raw ERP product, warehouse inventory, and partner contract
data into the curated INVENTORY schema tables.

Lineage:
  RAW.ERP_PRODUCTS            -> INVENTORY.SKUS
  RAW.ERP_PRODUCTS            -> INVENTORY.PRODUCT_CATEGORIES
  RAW.PARTNER_CONTRACTS       -> INVENTORY.DISTRIBUTORS
  RAW.ERP_WAREHOUSE_INVENTORY -> INVENTORY.WAREHOUSE_STOCK
"""

from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.empty import EmptyOperator

SQL_DIR = Path(__file__).parent / "sql" / "raw_to_inventory"

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email": ["data-alerts@company.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "snowflake_conn_id": "snowflake_enterprise_dw",
}

with DAG(
    dag_id="inventory_pipeline",
    default_args=default_args,
    description="RAW -> INVENTORY schema transformations (products, categories, distributors, stock)",
    schedule="0 6 * * *",  # Daily at 06:00 UTC
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["inventory", "daily", "raw-to-curated"],
    max_active_runs=1,
) as dag:

    start = EmptyOperator(task_id="start")

    # ── Product catalog ──────────────────────────────────────────────
    transform_skus = SnowflakeOperator(
        task_id="transform_skus",
        sql=(SQL_DIR / "transform_skus.sql").read_text(),
        database="ENTERPRISE_DW",
        schema="INVENTORY",
    )

    transform_product_categories = SnowflakeOperator(
        task_id="transform_product_categories",
        sql=(SQL_DIR / "transform_product_categories.sql").read_text(),
        database="ENTERPRISE_DW",
        schema="INVENTORY",
    )

    # ── Distribution partners ────────────────────────────────────────
    transform_distributors = SnowflakeOperator(
        task_id="transform_distributors",
        sql=(SQL_DIR / "transform_distributors.sql").read_text(),
        database="ENTERPRISE_DW",
        schema="INVENTORY",
    )

    # ── Warehouse stock levels ───────────────────────────────────────
    transform_warehouse_stock = SnowflakeOperator(
        task_id="transform_warehouse_stock",
        sql=(SQL_DIR / "transform_warehouse_stock.sql").read_text(),
        database="ENTERPRISE_DW",
        schema="INVENTORY",
    )

    # ── Row count checks ────────────────────────────────────────────
    validate_skus = SnowflakeOperator(
        task_id="validate_skus",
        sql="""
            SELECT CASE
                WHEN COUNT(*) > 0 THEN 'PASS'
                ELSE 'FAIL: SKUS table is empty after transform'
            END AS check_result
            FROM ENTERPRISE_DW.INVENTORY.SKUS
            WHERE STATUS = 'ACTIVE';
        """,
    )

    validate_stock = SnowflakeOperator(
        task_id="validate_stock",
        sql="""
            SELECT CASE
                WHEN COUNT(*) > 0 THEN 'PASS'
                ELSE 'FAIL: WAREHOUSE_STOCK has no records'
            END AS check_result
            FROM ENTERPRISE_DW.INVENTORY.WAREHOUSE_STOCK;
        """,
    )

    done = EmptyOperator(task_id="done")

    # ── Task dependencies ────────────────────────────────────────────
    # SKUs must load before categories (derived from product data)
    # and before stock (references SKU_ID)
    start >> transform_skus
    transform_skus >> transform_product_categories
    transform_skus >> transform_warehouse_stock >> validate_stock
    transform_skus >> validate_skus

    # Distributors are independent of SKUs
    start >> transform_distributors

    [validate_skus, validate_stock, transform_product_categories, transform_distributors] >> done

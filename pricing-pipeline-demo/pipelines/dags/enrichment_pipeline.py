"""
DAG: enrichment_pipeline
Schedule: Daily @ 08:00 UTC
Owner: data-engineering

Deploys the Nimble enrichment infrastructure and runs the distributor
price enrichment pipeline. Cross-joins INVENTORY.SKUS x INVENTORY.DISTRIBUTORS
and calls the nimble_enrich() UDTF to fetch live pricing from each
retailer's website.

Lineage:
  INVENTORY.SKUS + INVENTORY.DISTRIBUTORS -> INVENTORY.DISTRIBUTOR_PRICES
"""

from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.empty import EmptyOperator

SQL_DIR = Path(__file__).parent / "sql" / "enrichment"

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email": ["data-alerts@company.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "snowflake_conn_id": "snowflake_enterprise_dw",
}

with DAG(
    dag_id="enrichment_pipeline",
    default_args=default_args,
    description="Deploy Nimble UDTF and enrich distributor pricing via live web data",
    schedule="0 8 * * *",  # Daily at 08:00 UTC
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["enrichment", "pricing", "nimble", "daily"],
    max_active_runs=1,
) as dag:

    start = EmptyOperator(task_id="start")

    # ── Wait for upstream inventory data ─────────────────────────────
    wait_for_inventory = ExternalTaskSensor(
        task_id="wait_for_inventory_pipeline",
        external_dag_id="inventory_pipeline",
        external_task_id="done",
        mode="reschedule",
        timeout=3600,
        poke_interval=120,
    )

    # ── Snowflake infrastructure (idempotent) ────────────────────────
    setup_nimble_integration = SnowflakeOperator(
        task_id="setup_nimble_integration",
        sql=(SQL_DIR / "setup_nimble_integration.sql").read_text(),
    )

    # ── Deploy the UDTF ──────────────────────────────────────────────
    create_udtf = SnowflakeOperator(
        task_id="create_nimble_udtf",
        sql=(SQL_DIR / "create_nimble_udtf.sql").read_text(),
    )

    # ── Create output table ──────────────────────────────────────────
    create_output_table = SnowflakeOperator(
        task_id="create_distributor_prices_table",
        sql=(SQL_DIR / "create_distributor_prices.sql").read_text(),
        database="ENTERPRISE_DW",
        schema="INVENTORY",
    )

    # ── Run enrichment ───────────────────────────────────────────────
    enrich_distributor_prices = SnowflakeOperator(
        task_id="enrich_distributor_prices",
        sql=(SQL_DIR / "enrich_distributor_prices.sql").read_text(),
        database="ENTERPRISE_DW",
        schema="INVENTORY",
        execution_timeout=timedelta(hours=2),
    )

    # ── Validation ───────────────────────────────────────────────────
    validate_row_count = SnowflakeOperator(
        task_id="validate_row_count",
        sql="""
            -- Expected: active_skus * active_distributors rows
            WITH expected AS (
                SELECT
                    (SELECT COUNT(*) FROM ENTERPRISE_DW.INVENTORY.SKUS WHERE STATUS = 'ACTIVE') *
                    (SELECT COUNT(*) FROM ENTERPRISE_DW.INVENTORY.DISTRIBUTORS WHERE STATUS = 'ACTIVE') AS expected_rows
            )
            SELECT CASE
                WHEN (SELECT COUNT(*) FROM ENTERPRISE_DW.INVENTORY.DISTRIBUTOR_PRICES) = e.expected_rows
                THEN 'PASS: ' || e.expected_rows || ' rows as expected'
                ELSE 'WARN: Expected ' || e.expected_rows || ' but got ' ||
                     (SELECT COUNT(*) FROM ENTERPRISE_DW.INVENTORY.DISTRIBUTOR_PRICES)
            END AS check_result
            FROM expected e;
        """,
    )

    validate_no_nulls = SnowflakeOperator(
        task_id="validate_no_nulls",
        sql="""
            -- At least 80% of rows should have a non-null LISTED_PRICE
            SELECT CASE
                WHEN (SELECT COUNT(*) FROM ENTERPRISE_DW.INVENTORY.DISTRIBUTOR_PRICES WHERE LISTED_PRICE IS NOT NULL)
                     >= 0.8 * (SELECT COUNT(*) FROM ENTERPRISE_DW.INVENTORY.DISTRIBUTOR_PRICES)
                THEN 'PASS'
                ELSE 'WARN: More than 20% of prices are NULL'
            END AS check_result;
        """,
    )

    validate_price_range = SnowflakeOperator(
        task_id="validate_price_range",
        sql="""
            -- No prices should be negative or wildly above MSRP (>10x)
            SELECT CASE
                WHEN COUNT(*) = 0 THEN 'PASS'
                ELSE 'FAIL: ' || COUNT(*) || ' rows with suspicious prices'
            END AS check_result
            FROM ENTERPRISE_DW.INVENTORY.DISTRIBUTOR_PRICES
            WHERE LISTED_PRICE < 0
               OR LISTED_PRICE > MSRP * 10;
        """,
    )

    done = EmptyOperator(task_id="done")

    # ── Task dependencies ────────────────────────────────────────────
    start >> [setup_nimble_integration, wait_for_inventory]

    [setup_nimble_integration, wait_for_inventory] >> create_udtf >> create_output_table

    create_output_table >> enrich_distributor_prices

    enrich_distributor_prices >> [validate_row_count, validate_no_nulls, validate_price_range] >> done
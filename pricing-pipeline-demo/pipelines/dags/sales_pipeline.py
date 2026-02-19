"""
DAG: sales_pipeline
Schedule: Every 30 minutes
Owner: data-engineering

Transforms raw ERP sales order data and POS transactions into the
curated SALES schema tables. The orders/order_items run frequently
for near-realtime visibility, while the daily summary runs once per day.

Lineage:
  RAW.ERP_SALES_ORDERS -> SALES.ORDERS
  RAW.ERP_SALES_ORDERS -> SALES.ORDER_ITEMS
  RAW.POS_TRANSACTIONS -> SALES.DAILY_SALES_SUMMARY
"""

from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.empty import EmptyOperator

SQL_DIR = Path(__file__).parent / "sql" / "raw_to_sales"

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email": ["data-alerts@company.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=3),
    "snowflake_conn_id": "snowflake_enterprise_dw",
}

with DAG(
    dag_id="sales_pipeline",
    default_args=default_args,
    description="RAW -> SALES schema transformations (orders, line items, daily summary)",
    schedule="*/30 * * * *",  # Every 30 minutes
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["sales", "near-realtime", "raw-to-curated"],
    max_active_runs=1,
) as dag:

    start = EmptyOperator(task_id="start")

    # ── Order headers ────────────────────────────────────────────────
    transform_orders = SnowflakeOperator(
        task_id="transform_orders",
        sql=(SQL_DIR / "transform_orders.sql").read_text(),
        database="ENTERPRISE_DW",
        schema="SALES",
    )

    # ── Order line items ─────────────────────────────────────────────
    transform_order_items = SnowflakeOperator(
        task_id="transform_order_items",
        sql=(SQL_DIR / "transform_order_items.sql").read_text(),
        database="ENTERPRISE_DW",
        schema="SALES",
    )

    # ── Daily sales summary (POS aggregation) ────────────────────────
    transform_daily_sales_summary = SnowflakeOperator(
        task_id="transform_daily_sales_summary",
        sql=(SQL_DIR / "transform_daily_sales_summary.sql").read_text(),
        database="ENTERPRISE_DW",
        schema="SALES",
    )

    # ── Validation ───────────────────────────────────────────────────
    validate_orders = SnowflakeOperator(
        task_id="validate_orders",
        sql="""
            -- Check for orphaned order items (items without matching order header)
            SELECT CASE
                WHEN COUNT(*) = 0 THEN 'PASS'
                ELSE 'WARN: ' || COUNT(*) || ' orphaned order items found'
            END AS check_result
            FROM ENTERPRISE_DW.SALES.ORDER_ITEMS oi
            LEFT JOIN ENTERPRISE_DW.SALES.ORDERS o ON o.ORDER_ID = oi.ORDER_ID
            WHERE o.ORDER_ID IS NULL;
        """,
    )

    validate_revenue = SnowflakeOperator(
        task_id="validate_revenue",
        sql="""
            -- Sanity check: daily revenue should not exceed $500K per SKU
            SELECT CASE
                WHEN MAX(GROSS_REVENUE) < 500000 THEN 'PASS'
                ELSE 'WARN: Unusually high daily revenue detected'
            END AS check_result
            FROM ENTERPRISE_DW.SALES.DAILY_SALES_SUMMARY
            WHERE SALE_DATE >= DATEADD(day, -7, CURRENT_DATE());
        """,
    )

    done = EmptyOperator(task_id="done")

    # ── Task dependencies ────────────────────────────────────────────
    # Orders must load before order items (FK dependency)
    start >> transform_orders >> transform_order_items >> validate_orders

    # Daily summary can run in parallel with orders
    start >> transform_daily_sales_summary >> validate_revenue

    [validate_orders, validate_revenue] >> done

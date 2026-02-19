"""
DAG: supply_chain_pipeline
Schedule: Daily @ 06:00 UTC
Owner: data-engineering

Transforms raw SAP procurement data into the curated SUPPLY_CHAIN
schema tables.

Lineage:
  RAW.SAP_VENDORS          -> SUPPLY_CHAIN.SUPPLIERS
  RAW.SAP_PURCHASE_ORDERS  -> SUPPLY_CHAIN.PURCHASE_ORDERS
"""

from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.empty import EmptyOperator

SQL_DIR = Path(__file__).parent / "sql" / "raw_to_supply_chain"

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
    dag_id="supply_chain_pipeline",
    default_args=default_args,
    description="RAW -> SUPPLY_CHAIN schema transformations (suppliers, purchase orders)",
    schedule="0 6 * * *",  # Daily at 06:00 UTC
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["supply-chain", "sap", "daily", "raw-to-curated"],
    max_active_runs=1,
) as dag:

    start = EmptyOperator(task_id="start")

    # ── Supplier dimension ───────────────────────────────────────────
    transform_suppliers = SnowflakeOperator(
        task_id="transform_suppliers",
        sql=(SQL_DIR / "transform_suppliers.sql").read_text(),
        database="ENTERPRISE_DW",
        schema="SUPPLY_CHAIN",
    )

    # ── Purchase orders ──────────────────────────────────────────────
    transform_purchase_orders = SnowflakeOperator(
        task_id="transform_purchase_orders",
        sql=(SQL_DIR / "transform_purchase_orders.sql").read_text(),
        database="ENTERPRISE_DW",
        schema="SUPPLY_CHAIN",
    )

    # ── Validation ───────────────────────────────────────────────────
    validate_supplier_fk = SnowflakeOperator(
        task_id="validate_supplier_fk",
        sql="""
            -- Ensure all POs reference valid suppliers
            SELECT CASE
                WHEN COUNT(*) = 0 THEN 'PASS'
                ELSE 'FAIL: ' || COUNT(*) || ' POs with unknown supplier'
            END AS check_result
            FROM ENTERPRISE_DW.SUPPLY_CHAIN.PURCHASE_ORDERS po
            LEFT JOIN ENTERPRISE_DW.SUPPLY_CHAIN.SUPPLIERS s ON s.SUPPLIER_ID = po.SUPPLIER_ID
            WHERE s.SUPPLIER_ID IS NULL;
        """,
    )

    validate_po_amounts = SnowflakeOperator(
        task_id="validate_po_amounts",
        sql="""
            -- Sanity check: no negative quantities or costs
            SELECT CASE
                WHEN COUNT(*) = 0 THEN 'PASS'
                ELSE 'FAIL: ' || COUNT(*) || ' POs with negative values'
            END AS check_result
            FROM ENTERPRISE_DW.SUPPLY_CHAIN.PURCHASE_ORDERS
            WHERE QUANTITY < 0 OR UNIT_COST < 0;
        """,
    )

    done = EmptyOperator(task_id="done")

    # ── Task dependencies ────────────────────────────────────────────
    # Suppliers must load before POs (FK dependency)
    start >> transform_suppliers >> transform_purchase_orders
    transform_purchase_orders >> [validate_supplier_fk, validate_po_amounts] >> done

"""
DAG: crm_pipeline
Schedule: Every 6 hours (0 */6 * * *)
Owner: data-engineering

Transforms raw Salesforce CRM data into the curated CRM schema tables.
Runs every 6 hours to match the Fivetran Salesforce sync cadence.

Lineage:
  RAW.SALESFORCE_ACCOUNTS -> CRM.CUSTOMERS
  RAW.SALESFORCE_CASES    -> CRM.SUPPORT_TICKETS
"""

from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.empty import EmptyOperator

SQL_DIR = Path(__file__).parent / "sql" / "raw_to_crm"

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
    dag_id="crm_pipeline",
    default_args=default_args,
    description="RAW -> CRM schema transformations (customers, support tickets)",
    schedule="0 */6 * * *",  # Every 6 hours
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["crm", "salesforce", "raw-to-curated"],
    max_active_runs=1,
) as dag:

    start = EmptyOperator(task_id="start")

    # ── Customer dimension ───────────────────────────────────────────
    transform_customers = SnowflakeOperator(
        task_id="transform_customers",
        sql=(SQL_DIR / "transform_customers.sql").read_text(),
        database="ENTERPRISE_DW",
        schema="CRM",
    )

    # ── Support tickets ──────────────────────────────────────────────
    transform_support_tickets = SnowflakeOperator(
        task_id="transform_support_tickets",
        sql=(SQL_DIR / "transform_support_tickets.sql").read_text(),
        database="ENTERPRISE_DW",
        schema="CRM",
    )

    # ── Validation ───────────────────────────────────────────────────
    validate_no_deleted_customers = SnowflakeOperator(
        task_id="validate_no_deleted_customers",
        sql="""
            -- Ensure we didn't accidentally load soft-deleted Salesforce records
            SELECT CASE
                WHEN COUNT(*) = (
                    SELECT COUNT(*) FROM ENTERPRISE_DW.RAW.SALESFORCE_ACCOUNTS
                    WHERE IS_DELETED = FALSE AND ACCOUNT_TYPE ILIKE 'Customer%%'
                )
                THEN 'PASS'
                ELSE 'WARN: Customer count mismatch vs source'
            END AS check_result
            FROM ENTERPRISE_DW.CRM.CUSTOMERS;
        """,
    )

    validate_ticket_customer_fk = SnowflakeOperator(
        task_id="validate_ticket_customer_fk",
        sql="""
            -- Check that all tickets reference valid customers
            SELECT CASE
                WHEN COUNT(*) = 0 THEN 'PASS'
                ELSE 'WARN: ' || COUNT(*) || ' tickets with unknown customer'
            END AS check_result
            FROM ENTERPRISE_DW.CRM.SUPPORT_TICKETS t
            LEFT JOIN ENTERPRISE_DW.CRM.CUSTOMERS c ON c.CUSTOMER_ID = t.CUSTOMER_ID
            WHERE c.CUSTOMER_ID IS NULL
              AND t.CUSTOMER_ID IS NOT NULL;
        """,
    )

    done = EmptyOperator(task_id="done")

    # ── Task dependencies ────────────────────────────────────────────
    # Customers must load before tickets (FK lookup in transform)
    start >> transform_customers >> validate_no_deleted_customers
    transform_customers >> transform_support_tickets >> validate_ticket_customer_fk

    [validate_no_deleted_customers, validate_ticket_customer_fk] >> done

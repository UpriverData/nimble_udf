"""
DAG: finance_pipeline
Schedule: Daily @ 07:30 UTC
Owner: data-engineering

Transforms raw NetSuite financial data into the curated FINANCE schema
tables. Runs after the sales pipeline to ensure DAILY_SALES_SUMMARY
is up to date before P&L aggregation.

Lineage:
  RAW.NETSUITE_GL              -> FINANCE.GL_ACCOUNTS
  RAW.NETSUITE_JOURNAL_ENTRIES -> FINANCE.MONTHLY_PNL
"""

from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.empty import EmptyOperator

SQL_DIR = Path(__file__).parent / "sql" / "raw_to_finance"

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email": ["data-alerts@company.com", "finance-team@company.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "snowflake_conn_id": "snowflake_enterprise_dw",
}

with DAG(
    dag_id="finance_pipeline",
    default_args=default_args,
    description="RAW -> FINANCE schema transformations (GL accounts, monthly P&L)",
    schedule="30 7 * * *",  # Daily at 07:30 UTC
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["finance", "netsuite", "daily", "raw-to-curated"],
    max_active_runs=1,
) as dag:

    start = EmptyOperator(task_id="start")

    # ── Wait for upstream pipelines ──────────────────────────────────
    wait_for_sales = ExternalTaskSensor(
        task_id="wait_for_sales_pipeline",
        external_dag_id="sales_pipeline",
        external_task_id="done",
        mode="reschedule",
        timeout=3600,
        poke_interval=120,
    )

    # ── Chart of accounts ────────────────────────────────────────────
    transform_gl_accounts = SnowflakeOperator(
        task_id="transform_gl_accounts",
        sql=(SQL_DIR / "transform_gl_accounts.sql").read_text(),
        database="ENTERPRISE_DW",
        schema="FINANCE",
    )

    # ── Monthly P&L ──────────────────────────────────────────────────
    transform_monthly_pnl = SnowflakeOperator(
        task_id="transform_monthly_pnl",
        sql=(SQL_DIR / "transform_monthly_pnl.sql").read_text(),
        database="ENTERPRISE_DW",
        schema="FINANCE",
    )

    # ── Validation ───────────────────────────────────────────────────
    validate_pnl_balance = SnowflakeOperator(
        task_id="validate_pnl_balance",
        sql="""
            -- Revenue should always exceed COGS for a healthy business
            WITH period_totals AS (
                SELECT
                    p.PERIOD,
                    SUM(CASE WHEN g.ACCOUNT_TYPE = 'REVENUE' THEN p.AMOUNT ELSE 0 END) AS total_revenue,
                    SUM(CASE WHEN g.ACCOUNT_TYPE = 'EXPENSE' THEN p.AMOUNT ELSE 0 END) AS total_expense
                FROM ENTERPRISE_DW.FINANCE.MONTHLY_PNL p
                JOIN ENTERPRISE_DW.FINANCE.GL_ACCOUNTS g ON g.ACCOUNT_ID = p.ACCOUNT_ID
                GROUP BY p.PERIOD
            )
            SELECT CASE
                WHEN MIN(total_revenue - total_expense) > 0 THEN 'PASS'
                ELSE 'WARN: Some periods show negative gross margin'
            END AS check_result
            FROM period_totals;
        """,
    )

    validate_gl_coverage = SnowflakeOperator(
        task_id="validate_gl_coverage",
        sql="""
            -- Every P&L entry should reference a valid GL account
            SELECT CASE
                WHEN COUNT(*) = 0 THEN 'PASS'
                ELSE 'FAIL: ' || COUNT(*) || ' P&L entries with unknown GL account'
            END AS check_result
            FROM ENTERPRISE_DW.FINANCE.MONTHLY_PNL p
            LEFT JOIN ENTERPRISE_DW.FINANCE.GL_ACCOUNTS g ON g.ACCOUNT_ID = p.ACCOUNT_ID
            WHERE g.ACCOUNT_ID IS NULL;
        """,
    )

    done = EmptyOperator(task_id="done")

    # ── Task dependencies ────────────────────────────────────────────
    # GL accounts are independent, but P&L needs both GL and sales data
    start >> transform_gl_accounts
    start >> wait_for_sales

    [transform_gl_accounts, wait_for_sales] >> transform_monthly_pnl

    transform_monthly_pnl >> [validate_pnl_balance, validate_gl_coverage] >> done

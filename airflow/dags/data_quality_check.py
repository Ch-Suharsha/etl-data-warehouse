"""
Airflow DAG — Data Quality Check

Separate monitoring DAG that runs daily after the main ETL pipeline.
Checks: null rates, duplicate counts, referential integrity violations.
Logs all results to etl_run_log.

Schedule: Daily at 1 AM (1 hour after main ETL)
"""

import os
import sys
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from loaders.warehouse_loader import log_etl_run

logger = logging.getLogger(__name__)


def _warehouse_url():
    return (
        f"postgresql+psycopg2://{os.getenv('WAREHOUSE_USER', 'warehouse_user')}"
        f":{os.getenv('WAREHOUSE_PASSWORD', 'warehouse_pass')}"
        f"@{os.getenv('WAREHOUSE_HOST', 'localhost')}"
        f":{os.getenv('WAREHOUSE_PORT', '5432')}"
        f"/{os.getenv('WAREHOUSE_DB', 'analytics_warehouse')}"
    )


# ---------------------------------------------------------------------------
# Quality Check Tasks
# ---------------------------------------------------------------------------

def check_null_rates(**context):
    """Check null rates per critical column across all tables."""
    from sqlalchemy import create_engine, text

    engine = create_engine(_warehouse_url())
    issues = []

    checks = [
        ("fact_orders", "order_id"),
        ("fact_orders", "customer_key"),
        ("fact_orders", "date_key"),
        ("fact_orders", "total_amount"),
        ("dim_customers", "customer_id"),
        ("dim_customers", "email"),
        ("dim_products", "product_id"),
    ]

    with engine.connect() as conn:
        for table, column in checks:
            total = conn.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar()
            nulls = conn.execute(
                text(f"SELECT COUNT(*) FROM {table} WHERE {column} IS NULL")
            ).scalar()

            null_rate = (nulls / total * 100) if total > 0 else 0
            logger.info(f"  {table}.{column}: {nulls}/{total} nulls ({null_rate:.1f}%)")

            if null_rate > 5:  # Flag if >5% nulls
                issues.append(f"{table}.{column} has {null_rate:.1f}% nulls")

    context["ti"].xcom_push(key="null_issues", value=issues)
    logger.info(f"Null check complete — {len(issues)} issues found")


def check_duplicates(**context):
    """Check for duplicate records in key tables."""
    from sqlalchemy import create_engine, text

    engine = create_engine(_warehouse_url())
    issues = []

    dup_checks = [
        ("fact_orders", "order_id"),
        ("dim_customers", "customer_id"),
        ("dim_products", "product_id"),
    ]

    with engine.connect() as conn:
        for table, key_col in dup_checks:
            dupes = conn.execute(text(f"""
                SELECT COUNT(*) FROM (
                    SELECT {key_col}, COUNT(*) AS cnt
                    FROM {table}
                    GROUP BY {key_col}
                    HAVING COUNT(*) > 1
                ) sub
            """)).scalar()

            logger.info(f"  {table}: {dupes} duplicate {key_col}s")
            if dupes > 0:
                issues.append(f"{table} has {dupes} duplicate {key_col}s")

    context["ti"].xcom_push(key="dup_issues", value=issues)
    logger.info(f"Duplicate check complete — {len(issues)} issues found")


def check_referential_integrity(**context):
    """Check FK relationships between fact and dimension tables."""
    from sqlalchemy import create_engine, text

    engine = create_engine(_warehouse_url())
    issues = []

    fk_checks = [
        ("fact_orders", "customer_key", "dim_customers", "customer_key"),
        ("fact_orders", "product_key", "dim_products", "product_key"),
        ("fact_orders", "date_key", "dim_date", "date_key"),
    ]

    with engine.connect() as conn:
        for fact_tbl, fact_col, dim_tbl, dim_col in fk_checks:
            orphans = conn.execute(text(f"""
                SELECT COUNT(*)
                FROM {fact_tbl} f
                LEFT JOIN {dim_tbl} d ON f.{fact_col} = d.{dim_col}
                WHERE f.{fact_col} IS NOT NULL AND d.{dim_col} IS NULL
            """)).scalar()

            logger.info(f"  {fact_tbl}.{fact_col} → {dim_tbl}.{dim_col}: {orphans} orphans")
            if orphans > 0:
                issues.append(
                    f"{orphans} rows in {fact_tbl} have orphaned {fact_col} "
                    f"(no match in {dim_tbl})"
                )

    context["ti"].xcom_push(key="fk_issues", value=issues)
    logger.info(f"Referential integrity check complete — {len(issues)} issues found")


def log_quality_results(**context):
    """Aggregate all quality check results and log to etl_run_log."""
    ti = context["ti"]
    all_issues = []
    all_issues.extend(ti.xcom_pull(key="null_issues") or [])
    all_issues.extend(ti.xcom_pull(key="dup_issues") or [])
    all_issues.extend(ti.xcom_pull(key="fk_issues") or [])

    status = "SUCCESS" if not all_issues else "WARNING"
    error_msg = "; ".join(all_issues) if all_issues else None

    run_details = {
        "dag_id": "data_quality_check",
        "source_name": "warehouse_audit",
        "records_extracted": 0,
        "records_transformed": 0,
        "records_loaded": 0,
        "records_rejected": len(all_issues),
        "status": status,
        "error_message": error_msg,
    }

    log_etl_run(run_details, _warehouse_url())
    logger.info(f"Quality results logged — status: {status}, issues: {len(all_issues)}")


# ---------------------------------------------------------------------------
# DAG Definition
# ---------------------------------------------------------------------------

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

with DAG(
    dag_id="data_quality_check",
    default_args=default_args,
    description="Daily data quality monitoring for the analytics warehouse",
    schedule_interval="0 1 * * *",  # 1 AM daily
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["quality", "monitoring", "warehouse"],
) as dag:

    t_null_check = PythonOperator(
        task_id="check_null_rates",
        python_callable=check_null_rates,
    )

    t_dup_check = PythonOperator(
        task_id="check_duplicates",
        python_callable=check_duplicates,
    )

    t_fk_check = PythonOperator(
        task_id="check_referential_integrity",
        python_callable=check_referential_integrity,
    )

    t_log_results = PythonOperator(
        task_id="log_quality_results",
        python_callable=log_quality_results,
    )

    # Run all checks in parallel, then log
    [t_null_check, t_dup_check, t_fk_check] >> t_log_results

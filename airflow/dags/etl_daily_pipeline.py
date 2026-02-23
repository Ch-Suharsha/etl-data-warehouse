"""
Airflow DAG — ETL Daily Pipeline

Orchestrates the full ETL workflow:
  extract (3 sources) → transform & validate → load dimensions →
  load facts → run dbt models → quality checks → log completion

Schedule: Daily at midnight
Retries: 2 with 5-minute delay
"""

import os
import sys
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

# ---------------------------------------------------------------------------
# Add project root to path so our modules are importable
# ---------------------------------------------------------------------------
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from extractors.postgres_extractor import extract_orders
from extractors.mysql_extractor import extract_customers
from extractors.mongo_extractor import extract_reviews
from transformers.data_transformer import (
    clean_orders,
    clean_customers,
    clean_reviews,
    validate_referential_integrity,
)
from loaders.warehouse_loader import (
    load_dimension,
    load_fact,
    populate_date_dimension,
    log_etl_run,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Connection URLs (from environment variables set in docker-compose)
# ---------------------------------------------------------------------------

def _pg_source_url():
    return (
        f"postgresql+psycopg2://{os.getenv('PG_SOURCE_USER', 'source_user')}"
        f":{os.getenv('PG_SOURCE_PASSWORD', 'source_pass')}"
        f"@{os.getenv('PG_SOURCE_HOST', 'localhost')}"
        f":{os.getenv('PG_SOURCE_PORT', '5433')}"
        f"/{os.getenv('PG_SOURCE_DB', 'ecommerce_orders')}"
    )

def _mysql_source_url():
    return (
        f"mysql+pymysql://{os.getenv('MYSQL_SOURCE_USER', 'source_user')}"
        f":{os.getenv('MYSQL_SOURCE_PASSWORD', 'source_pass')}"
        f"@{os.getenv('MYSQL_SOURCE_HOST', 'localhost')}"
        f":{os.getenv('MYSQL_SOURCE_PORT', '3307')}"
        f"/{os.getenv('MYSQL_SOURCE_DB', 'customer_management')}"
    )

def _warehouse_url():
    return (
        f"postgresql+psycopg2://{os.getenv('WAREHOUSE_USER', 'warehouse_user')}"
        f":{os.getenv('WAREHOUSE_PASSWORD', 'warehouse_pass')}"
        f"@{os.getenv('WAREHOUSE_HOST', 'localhost')}"
        f":{os.getenv('WAREHOUSE_PORT', '5432')}"
        f"/{os.getenv('WAREHOUSE_DB', 'analytics_warehouse')}"
    )


# ---------------------------------------------------------------------------
# Task callables
# ---------------------------------------------------------------------------

def task_extract_orders(**context):
    """Extract orders from PostgreSQL source."""
    df = extract_orders(_pg_source_url())
    # Save to temp parquet for downstream tasks
    path = "/tmp/etl_orders.parquet"
    df.to_parquet(path, index=False)
    context["ti"].xcom_push(key="orders_path", value=path)
    context["ti"].xcom_push(key="orders_count", value=len(df))
    logger.info(f"Extracted {len(df):,} orders → {path}")


def task_extract_customers(**context):
    """Extract customers from MySQL source."""
    df = extract_customers(_mysql_source_url())
    path = "/tmp/etl_customers.parquet"
    df.to_parquet(path, index=False)
    context["ti"].xcom_push(key="customers_path", value=path)
    context["ti"].xcom_push(key="customers_count", value=len(df))
    logger.info(f"Extracted {len(df):,} customers → {path}")


def task_extract_reviews(**context):
    """Extract reviews from MongoDB source."""
    df = extract_reviews(
        host=os.getenv("MONGO_SOURCE_HOST", "localhost"),
        port=int(os.getenv("MONGO_SOURCE_PORT", "27017")),
        db_name=os.getenv("MONGO_SOURCE_DB", "product_reviews"),
    )
    path = "/tmp/etl_reviews.parquet"
    df.to_parquet(path, index=False)
    context["ti"].xcom_push(key="reviews_path", value=path)
    context["ti"].xcom_push(key="reviews_count", value=len(df))
    logger.info(f"Extracted {len(df):,} reviews → {path}")


def task_transform_and_validate(**context):
    """Clean all 3 DataFrames and validate referential integrity."""
    import pandas as pd

    ti = context["ti"]
    orders_df = pd.read_parquet(ti.xcom_pull(key="orders_path"))
    customers_df = pd.read_parquet(ti.xcom_pull(key="customers_path"))
    reviews_df = pd.read_parquet(ti.xcom_pull(key="reviews_path"))

    # Clean
    orders_clean = clean_orders(orders_df)
    customers_clean = clean_customers(customers_df)
    reviews_clean = clean_reviews(reviews_df)

    # Validate referential integrity
    orders_valid = validate_referential_integrity(orders_clean, customers_clean)

    # Compute rejected count
    rejected = len(orders_clean) - len(orders_valid)
    ti.xcom_push(key="orders_rejected", value=rejected)

    # Save cleaned data
    orders_valid.to_parquet("/tmp/etl_orders_clean.parquet", index=False)
    customers_clean.to_parquet("/tmp/etl_customers_clean.parquet", index=False)
    reviews_clean.to_parquet("/tmp/etl_reviews_clean.parquet", index=False)

    ti.xcom_push(key="orders_transformed", value=len(orders_valid))
    ti.xcom_push(key="customers_transformed", value=len(customers_clean))
    ti.xcom_push(key="reviews_transformed", value=len(reviews_clean))

    logger.info(
        f"Transform complete — orders: {len(orders_valid):,}, "
        f"customers: {len(customers_clean):,}, "
        f"reviews: {len(reviews_clean):,}, "
        f"rejected: {rejected:,}"
    )


def task_load_dimensions(**context):
    """Load dim_customers, dim_products, and dim_date."""
    import pandas as pd

    wh_url = _warehouse_url()

    # Load customers dimension
    customers_df = pd.read_parquet("/tmp/etl_customers_clean.parquet")
    dim_cols = [
        "customer_id", "first_name", "last_name", "email",
        "city", "state", "country", "customer_tier",
        "lifetime_value", "is_active", "account_age_days",
    ]
    load_dimension(customers_df[dim_cols], "dim_customers", "customer_id", wh_url)

    # Build product dimension from reviews
    reviews_df = pd.read_parquet("/tmp/etl_reviews_clean.parquet")
    products_df = (
        reviews_df.groupby(["product_id", "product_category"])
        .agg(avg_rating=("rating", "mean"), total_reviews=("rating", "count"))
        .reset_index()
    )
    products_df["avg_rating"] = products_df["avg_rating"].round(2)
    load_dimension(products_df, "dim_products", "product_id", wh_url)

    # Populate date dimension (last 2 years)
    from datetime import date, timedelta
    end = date.today()
    start = end - timedelta(days=730)
    populate_date_dimension(start, end, wh_url)

    logger.info("Dimensions loaded successfully")


def task_load_facts(**context):
    """Load fact_orders and fact_daily_sales."""
    import pandas as pd
    from sqlalchemy import create_engine, text

    wh_url = _warehouse_url()
    engine = create_engine(wh_url)

    orders_df = pd.read_parquet("/tmp/etl_orders_clean.parquet")

    # Resolve surrogate keys via lookups
    with engine.connect() as conn:
        cust_keys = pd.read_sql(
            text("SELECT customer_key, customer_id FROM dim_customers"), conn
        )
        prod_keys = pd.read_sql(
            text("SELECT product_key, product_id FROM dim_products"), conn
        )

    orders_fact = orders_df.merge(cust_keys, on="customer_id", how="left")
    orders_fact = orders_fact.merge(prod_keys, on="product_id", how="left")

    # Build date_key
    orders_fact["order_date"] = pd.to_datetime(orders_fact["order_date"])
    orders_fact["date_key"] = orders_fact["order_date"].dt.strftime("%Y%m%d").astype(int)

    fact_cols = [
        "order_id", "customer_key", "product_key", "date_key",
        "quantity", "unit_price", "total_amount", "status", "payment_method",
    ]
    load_fact(orders_fact[fact_cols], "fact_orders", wh_url)

    # Aggregate for daily sales
    daily = (
        orders_fact.groupby(["date_key", "product_key"])
        .agg(
            total_revenue=("total_amount", "sum"),
            total_orders=("order_id", "count"),
            avg_order_value=("total_amount", "mean"),
            cancelled_orders=("status", lambda x: (x == "CANCELLED").sum()),
            refunded_amount=("total_amount", lambda x: x[orders_fact.loc[x.index, "status"] == "REFUNDED"].sum()),
        )
        .reset_index()
    )
    daily = daily.round(2)
    load_fact(daily, "fact_daily_sales", wh_url)

    context["ti"].xcom_push(key="orders_loaded", value=len(orders_fact))
    logger.info(f"Facts loaded — {len(orders_fact):,} orders, {len(daily):,} daily rows")


def task_run_dbt(**context):
    """Run dbt models and tests."""
    import subprocess

    dbt_dir = os.path.join(PROJECT_ROOT, "dbt_warehouse")

    # dbt run
    result_run = subprocess.run(
        ["dbt", "run", "--project-dir", dbt_dir, "--profiles-dir", dbt_dir],
        capture_output=True, text=True,
    )
    logger.info(f"dbt run stdout:\n{result_run.stdout}")
    if result_run.returncode != 0:
        logger.error(f"dbt run stderr:\n{result_run.stderr}")
        raise RuntimeError("dbt run failed")

    # dbt test
    result_test = subprocess.run(
        ["dbt", "test", "--project-dir", dbt_dir, "--profiles-dir", dbt_dir],
        capture_output=True, text=True,
    )
    logger.info(f"dbt test stdout:\n{result_test.stdout}")
    if result_test.returncode != 0:
        logger.error(f"dbt test stderr:\n{result_test.stderr}")
        raise RuntimeError("dbt test failed")


def task_quality_checks(**context):
    """Verify data quality in the warehouse."""
    import pandas as pd
    from sqlalchemy import create_engine, text

    engine = create_engine(_warehouse_url())
    issues = []

    with engine.connect() as conn:
        # Check fact_orders has records
        count = conn.execute(text("SELECT COUNT(*) FROM fact_orders")).scalar()
        if count == 0:
            issues.append("fact_orders is empty!")
        logger.info(f"Quality: fact_orders has {count:,} records")

        # Check for null keys
        null_keys = conn.execute(text(
            "SELECT COUNT(*) FROM fact_orders WHERE customer_key IS NULL"
        )).scalar()
        if null_keys > 0:
            issues.append(f"{null_keys} orders have null customer_key")

        # Check dim completeness
        dim_count = conn.execute(text("SELECT COUNT(*) FROM dim_customers")).scalar()
        logger.info(f"Quality: dim_customers has {dim_count:,} records")

        prod_count = conn.execute(text("SELECT COUNT(*) FROM dim_products")).scalar()
        logger.info(f"Quality: dim_products has {prod_count:,} records")

    if issues:
        logger.warning(f"Quality issues found: {issues}")
    else:
        logger.info("Quality checks passed ✓")

    context["ti"].xcom_push(key="quality_issues", value=issues)


def task_log_completion(**context):
    """Log the ETL run to the audit table."""
    ti = context["ti"]
    wh_url = _warehouse_url()

    run_details = {
        "dag_id": "etl_daily_pipeline",
        "source_name": "all_sources",
        "records_extracted": (
            (ti.xcom_pull(key="orders_count") or 0)
            + (ti.xcom_pull(key="customers_count") or 0)
            + (ti.xcom_pull(key="reviews_count") or 0)
        ),
        "records_transformed": (
            (ti.xcom_pull(key="orders_transformed") or 0)
            + (ti.xcom_pull(key="customers_transformed") or 0)
            + (ti.xcom_pull(key="reviews_transformed") or 0)
        ),
        "records_loaded": ti.xcom_pull(key="orders_loaded") or 0,
        "records_rejected": ti.xcom_pull(key="orders_rejected") or 0,
        "status": "SUCCESS",
    }

    quality_issues = ti.xcom_pull(key="quality_issues") or []
    if quality_issues:
        run_details["status"] = "WARNING"
        run_details["error_message"] = "; ".join(quality_issues)

    log_etl_run(run_details, wh_url)
    logger.info("ETL run logged ✓")


# ---------------------------------------------------------------------------
# DAG Definition
# ---------------------------------------------------------------------------

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="etl_daily_pipeline",
    default_args=default_args,
    description="Daily ETL pipeline: 3 sources → transform → star schema warehouse",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["etl", "warehouse", "production"],
) as dag:

    t_extract_orders = PythonOperator(
        task_id="extract_orders",
        python_callable=task_extract_orders,
    )

    t_extract_customers = PythonOperator(
        task_id="extract_customers",
        python_callable=task_extract_customers,
    )

    t_extract_reviews = PythonOperator(
        task_id="extract_reviews",
        python_callable=task_extract_reviews,
    )

    t_transform = PythonOperator(
        task_id="transform_and_validate",
        python_callable=task_transform_and_validate,
    )

    t_load_dims = PythonOperator(
        task_id="load_dimensions",
        python_callable=task_load_dimensions,
    )

    t_load_facts = PythonOperator(
        task_id="load_facts",
        python_callable=task_load_facts,
    )

    t_dbt = PythonOperator(
        task_id="run_dbt_models",
        python_callable=task_run_dbt,
    )

    t_quality = PythonOperator(
        task_id="run_quality_checks",
        python_callable=task_quality_checks,
    )

    t_log = PythonOperator(
        task_id="log_completion",
        python_callable=task_log_completion,
    )

    # Task dependencies (linear pipeline)
    (
        [t_extract_orders, t_extract_customers, t_extract_reviews]
        >> t_transform
        >> t_load_dims
        >> t_load_facts
        >> t_dbt
        >> t_quality
        >> t_log
    )

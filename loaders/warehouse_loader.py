"""
Warehouse Loader — Star Schema Loading Functions

Provides functions to load cleaned data into the analytics warehouse:
  1. load_dimension()          — Upsert dimension records
  2. load_fact()               — Bulk-insert fact records
  3. populate_date_dimension() — Generate and load the date dimension
  4. log_etl_run()             — Record ETL audit entries
"""

import logging
from datetime import datetime, timedelta, date

import pandas as pd
from sqlalchemy import create_engine, text

logger = logging.getLogger(__name__)


# -------------------------------------------------------------------
# 1. Dimension Loader (Upsert)
# -------------------------------------------------------------------

def load_dimension(
    df: pd.DataFrame,
    table_name: str,
    key_column: str,
    connection_url: str,
) -> dict:
    """
    Upsert dimension records using ON CONFLICT ... DO UPDATE.

    Parameters
    ----------
    df : pd.DataFrame
        Cleaned dimension data.
    table_name : str
        Target table (e.g. 'dim_customers', 'dim_products').
    key_column : str
        Business key column for conflict detection (e.g. 'customer_id').
    connection_url : str
        SQLAlchemy connection string for the warehouse.

    Returns
    -------
    dict
        {"inserted": int, "updated": int}
    """
    engine = create_engine(connection_url)

    if df.empty:
        logger.warning(f"Empty DataFrame — skipping load for {table_name}")
        return {"inserted": 0, "updated": 0}

    # Build column lists (exclude surrogate key)
    columns = [c for c in df.columns if c not in ("customer_key", "product_key", "date_key")]
    col_names = ", ".join(columns)
    col_params = ", ".join([f":{c}" for c in columns])
    update_set = ", ".join(
        [f"{c} = EXCLUDED.{c}" for c in columns if c != key_column]
    )

    upsert_sql = text(f"""
        INSERT INTO {table_name} ({col_names})
        VALUES ({col_params})
        ON CONFLICT ({key_column}) DO UPDATE SET
            {update_set},
            updated_at = CURRENT_TIMESTAMP
    """)

    records = df[columns].to_dict(orient="records")
    batch_size = 5000
    total = 0

    with engine.begin() as conn:
        for i in range(0, len(records), batch_size):
            batch = records[i : i + batch_size]
            conn.execute(upsert_sql, batch)
            total += len(batch)
            logger.info(f"  {table_name}: upserted {total:,}/{len(records):,}")

    logger.info(f"✓ {table_name}: loaded {total:,} dimension records")
    return {"inserted": total, "updated": 0}  # PG doesn't separate these


# -------------------------------------------------------------------
# 2. Fact Loader (Bulk Insert, skip dupes)
# -------------------------------------------------------------------

def load_fact(
    df: pd.DataFrame,
    table_name: str,
    connection_url: str,
) -> int:
    """
    Bulk-insert fact records, skipping duplicates.

    Parameters
    ----------
    df : pd.DataFrame
        Fact data with surrogate keys already resolved.
    table_name : str
        Target fact table (e.g. 'fact_orders').
    connection_url : str
        SQLAlchemy connection string for the warehouse.

    Returns
    -------
    int
        Number of records loaded.
    """
    engine = create_engine(connection_url)

    if df.empty:
        logger.warning(f"Empty DataFrame — skipping load for {table_name}")
        return 0

    # Determine conflict column
    conflict_col = "order_id" if table_name == "fact_orders" else "date_key, product_key"

    columns = [c for c in df.columns if c not in ("order_key",)]
    col_names = ", ".join(columns)
    col_params = ", ".join([f":{c}" for c in columns])

    insert_sql = text(f"""
        INSERT INTO {table_name} ({col_names})
        VALUES ({col_params})
        ON CONFLICT ({conflict_col}) DO NOTHING
    """)

    records = df[columns].to_dict(orient="records")
    batch_size = 5000
    total = 0

    with engine.begin() as conn:
        for i in range(0, len(records), batch_size):
            batch = records[i : i + batch_size]
            conn.execute(insert_sql, batch)
            total += len(batch)
            logger.info(f"  {table_name}: inserted {total:,}/{len(records):,}")

    logger.info(f"✓ {table_name}: loaded {total:,} fact records")
    return total


# -------------------------------------------------------------------
# 3. Date Dimension Populator
# -------------------------------------------------------------------

DAY_NAMES = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
MONTH_NAMES = [
    "", "January", "February", "March", "April", "May", "June",
    "July", "August", "September", "October", "November", "December",
]


def populate_date_dimension(
    start_date: date,
    end_date: date,
    connection_url: str,
) -> int:
    """
    Generate all dates between start and end, insert into dim_date.

    Parameters
    ----------
    start_date : date
        First date (inclusive).
    end_date : date
        Last date (inclusive).
    connection_url : str
        SQLAlchemy connection string for the warehouse.

    Returns
    -------
    int
        Number of date records loaded.
    """
    engine = create_engine(connection_url)

    insert_sql = text("""
        INSERT INTO dim_date
            (date_key, full_date, day_of_week, day_name,
             month, month_name, quarter, year, is_weekend)
        VALUES
            (:date_key, :full_date, :day_of_week, :day_name,
             :month, :month_name, :quarter, :year, :is_weekend)
        ON CONFLICT (date_key) DO NOTHING
    """)

    records = []
    current = start_date
    while current <= end_date:
        dow = current.weekday()  # 0=Monday
        records.append({
            "date_key": int(current.strftime("%Y%m%d")),
            "full_date": current,
            "day_of_week": dow,
            "day_name": DAY_NAMES[dow],
            "month": current.month,
            "month_name": MONTH_NAMES[current.month],
            "quarter": (current.month - 1) // 3 + 1,
            "year": current.year,
            "is_weekend": dow >= 5,
        })
        current += timedelta(days=1)

    with engine.begin() as conn:
        conn.execute(insert_sql, records)

    logger.info(f"✓ dim_date: populated {len(records):,} date records")
    return len(records)


# -------------------------------------------------------------------
# 4. ETL Run Logger
# -------------------------------------------------------------------

def log_etl_run(
    run_details: dict,
    connection_url: str,
) -> None:
    """
    Insert a record into etl_run_log for audit purposes.

    Parameters
    ----------
    run_details : dict
        Must contain keys: dag_id, source_name, records_extracted,
        records_transformed, records_loaded, records_rejected,
        status, error_message (optional).
    connection_url : str
        SQLAlchemy connection string for the warehouse.
    """
    engine = create_engine(connection_url)

    insert_sql = text("""
        INSERT INTO etl_run_log
            (dag_id, source_name, records_extracted, records_transformed,
             records_loaded, records_rejected, status, error_message)
        VALUES
            (:dag_id, :source_name, :records_extracted, :records_transformed,
             :records_loaded, :records_rejected, :status, :error_message)
    """)

    run_details.setdefault("error_message", None)

    with engine.begin() as conn:
        conn.execute(insert_sql, run_details)

    logger.info(
        f"✓ ETL run logged: {run_details['source_name']} — "
        f"{run_details['status']}"
    )

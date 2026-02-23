"""
MySQL Extractor — Customers

Extracts customer records from the MySQL source database.
Supports full and incremental extraction based on signup_date.
"""

import logging
from datetime import datetime
from typing import Optional

import pandas as pd
from sqlalchemy import create_engine, text

logger = logging.getLogger(__name__)


def extract_customers(
    connection_url: str,
    last_extracted_date: Optional[datetime] = None,
) -> pd.DataFrame:
    """
    Extract customers from MySQL source.

    Parameters
    ----------
    connection_url : str
        SQLAlchemy connection string for the customer_management database.
    last_extracted_date : datetime, optional
        If provided, only extracts customers with signup_date > this value
        (incremental). If None, does a full extraction.

    Returns
    -------
    pd.DataFrame
        DataFrame with all customer columns.
    """
    engine = create_engine(connection_url)

    if last_extracted_date:
        query = text("""
            SELECT customer_id, first_name, last_name, email, phone,
                   city, state, country, signup_date, customer_tier,
                   lifetime_value, is_active
            FROM customers
            WHERE signup_date > :last_date
            ORDER BY signup_date
        """)
        params = {"last_date": last_extracted_date}
        logger.info(f"Incremental extract: customers after {last_extracted_date}")
    else:
        query = text("""
            SELECT customer_id, first_name, last_name, email, phone,
                   city, state, country, signup_date, customer_tier,
                   lifetime_value, is_active
            FROM customers
            ORDER BY signup_date
        """)
        params = {}
        logger.info("Full extract: all customers")

    with engine.connect() as conn:
        df = pd.read_sql(query, conn, params=params)

    logger.info(f"Extracted {len(df):,} customer records from MySQL")
    return df

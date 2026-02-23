"""
PostgreSQL Extractor — Orders

Extracts order records from the PostgreSQL source database.
Supports full and incremental extraction based on order_date.
"""

import logging
from datetime import datetime
from typing import Optional

import pandas as pd
from sqlalchemy import create_engine, text

logger = logging.getLogger(__name__)


def extract_orders(
    connection_url: str,
    last_extracted_date: Optional[datetime] = None,
) -> pd.DataFrame:
    """
    Extract orders from PostgreSQL source.

    Parameters
    ----------
    connection_url : str
        SQLAlchemy connection string for the orders database.
    last_extracted_date : datetime, optional
        If provided, only extracts orders with order_date > this value
        (incremental). If None, does a full extraction.

    Returns
    -------
    pd.DataFrame
        DataFrame with all order columns.
    """
    engine = create_engine(connection_url)

    if last_extracted_date:
        query = text("""
            SELECT order_id, customer_id, product_id, order_date,
                   quantity, unit_price, total_amount, status,
                   payment_method, shipping_address
            FROM orders
            WHERE order_date > :last_date
            ORDER BY order_date
        """)
        params = {"last_date": last_extracted_date}
        logger.info(f"Incremental extract: orders after {last_extracted_date}")
    else:
        query = text("""
            SELECT order_id, customer_id, product_id, order_date,
                   quantity, unit_price, total_amount, status,
                   payment_method, shipping_address
            FROM orders
            ORDER BY order_date
        """)
        params = {}
        logger.info("Full extract: all orders")

    with engine.connect() as conn:
        df = pd.read_sql(query, conn, params=params)

    logger.info(f"Extracted {len(df):,} order records from PostgreSQL")
    return df

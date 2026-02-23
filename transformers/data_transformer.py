"""
Data Transformer — Cleaning & Validation Functions

Provides four core transformation functions used in the ETL pipeline:
  1. clean_orders()    — deduplicate, handle nulls, derive time columns
  2. clean_customers() — deduplicate, standardize, compute account age
  3. clean_reviews()   — deduplicate, validate ratings, add sentiment
  4. validate_referential_integrity() — ensure FK consistency
"""

import logging
from datetime import datetime

import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)


# -------------------------------------------------------------------
# 1. Orders Cleaning
# -------------------------------------------------------------------

def clean_orders(orders_df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean the raw orders DataFrame.

    Steps:
    - Remove duplicates by order_id
    - Handle null quantities (default to 1)
    - Recalculate total_amount where missing (quantity × unit_price)
    - Standardize status to uppercase
    - Add order_month, order_year, order_day_of_week columns
    """
    df = orders_df.copy()
    initial_count = len(df)

    # Deduplicate
    df = df.drop_duplicates(subset=["order_id"], keep="first")
    dupes_removed = initial_count - len(df)
    if dupes_removed:
        logger.info(f"Orders: removed {dupes_removed:,} duplicate rows")

    # Handle null quantities
    null_qty = df["quantity"].isna().sum()
    if null_qty:
        df["quantity"] = df["quantity"].fillna(1).astype(int)
        logger.info(f"Orders: filled {null_qty} null quantities with 1")

    # Recalculate total_amount where missing
    missing_total = df["total_amount"].isna()
    if missing_total.any():
        df.loc[missing_total, "total_amount"] = (
            df.loc[missing_total, "quantity"] * df.loc[missing_total, "unit_price"]
        )
        logger.info(f"Orders: recalculated {missing_total.sum()} missing total_amounts")

    # Standardize status
    df["status"] = df["status"].str.upper().str.strip()

    # Ensure order_date is datetime
    df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce")

    # Derive time columns
    df["order_month"] = df["order_date"].dt.month
    df["order_year"] = df["order_date"].dt.year
    df["order_day_of_week"] = df["order_date"].dt.dayofweek  # 0=Monday

    logger.info(f"Orders cleaned: {len(df):,} records ({initial_count:,} raw)")
    return df


# -------------------------------------------------------------------
# 2. Customers Cleaning
# -------------------------------------------------------------------

def clean_customers(customers_df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean the raw customers DataFrame.

    Steps:
    - Remove duplicates by customer_id
    - Standardize email to lowercase
    - Handle null phone numbers (set to 'N/A')
    - Validate customer_tier values
    - Add account_age_days column
    """
    df = customers_df.copy()
    initial_count = len(df)

    # Deduplicate
    df = df.drop_duplicates(subset=["customer_id"], keep="first")
    dupes_removed = initial_count - len(df)
    if dupes_removed:
        logger.info(f"Customers: removed {dupes_removed:,} duplicate rows")

    # Standardize email
    df["email"] = df["email"].str.lower().str.strip()

    # Handle null phones
    null_phones = df["phone"].isna().sum()
    if null_phones:
        df["phone"] = df["phone"].fillna("N/A")
        logger.info(f"Customers: filled {null_phones} null phone numbers with 'N/A'")

    # Validate tier
    valid_tiers = {"BRONZE", "SILVER", "GOLD", "PLATINUM"}
    df["customer_tier"] = df["customer_tier"].str.upper().str.strip()
    invalid_tiers = ~df["customer_tier"].isin(valid_tiers)
    if invalid_tiers.any():
        logger.warning(
            f"Customers: {invalid_tiers.sum()} rows have invalid tiers, "
            f"defaulting to BRONZE"
        )
        df.loc[invalid_tiers, "customer_tier"] = "BRONZE"

    # Compute account age
    df["signup_date"] = pd.to_datetime(df["signup_date"], errors="coerce")
    df["account_age_days"] = (datetime.now() - df["signup_date"]).dt.days

    logger.info(f"Customers cleaned: {len(df):,} records ({initial_count:,} raw)")
    return df


# -------------------------------------------------------------------
# 3. Reviews Cleaning
# -------------------------------------------------------------------

def clean_reviews(reviews_df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean the raw reviews DataFrame.

    Steps:
    - Remove duplicates by review_id
    - Validate rating is 1–5
    - Handle null review_text (set to empty string)
    - Parse review_date to consistent datetime
    - Add sentiment_category (1–2: negative, 3: neutral, 4–5: positive)
    """
    df = reviews_df.copy()
    initial_count = len(df)

    # Deduplicate
    df = df.drop_duplicates(subset=["review_id"], keep="first")
    dupes_removed = initial_count - len(df)
    if dupes_removed:
        logger.info(f"Reviews: removed {dupes_removed:,} duplicate rows")

    # Validate rating range
    out_of_range = ~df["rating"].between(1, 5)
    if out_of_range.any():
        logger.warning(
            f"Reviews: clipping {out_of_range.sum()} ratings to [1, 5]"
        )
        df["rating"] = df["rating"].clip(1, 5)

    # Handle null review_text
    df["review_text"] = df["review_text"].fillna("")

    # Parse review_date
    df["review_date"] = pd.to_datetime(df["review_date"], errors="coerce")

    # Add sentiment category
    df["sentiment_category"] = pd.cut(
        df["rating"],
        bins=[0, 2, 3, 5],
        labels=["negative", "neutral", "positive"],
        include_lowest=True,
    )

    logger.info(f"Reviews cleaned: {len(df):,} records ({initial_count:,} raw)")
    return df


# -------------------------------------------------------------------
# 4. Referential Integrity Validation
# -------------------------------------------------------------------

def validate_referential_integrity(
    orders_df: pd.DataFrame,
    customers_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Validate that all customer_ids in orders exist in the customers table.

    Parameters
    ----------
    orders_df : pd.DataFrame
        Cleaned orders (must have 'customer_id' column).
    customers_df : pd.DataFrame
        Cleaned customers (must have 'customer_id' column).

    Returns
    -------
    pd.DataFrame
        Orders with only valid customer references.
    """
    valid_customer_ids = set(customers_df["customer_id"].unique())
    total_orders = len(orders_df)

    # Find orphaned records
    orphaned = ~orders_df["customer_id"].isin(valid_customer_ids)
    orphan_count = orphaned.sum()

    if orphan_count:
        logger.warning(
            f"Referential integrity: {orphan_count:,} orders reference "
            f"non-existent customers ({orphan_count / total_orders:.1%}). "
            f"Removing orphaned records."
        )
        orders_df = orders_df[~orphaned].copy()
    else:
        logger.info("Referential integrity: all orders have valid customer references")

    logger.info(
        f"Referential integrity check: {len(orders_df):,}/{total_orders:,} "
        f"orders retained"
    )
    return orders_df

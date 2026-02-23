"""
MongoDB Extractor — Product Reviews

Extracts product review documents from the MongoDB source database.
Flattens documents into a pandas DataFrame. Supports full and
incremental extraction based on review_date.
"""

import logging
from datetime import datetime
from typing import Optional

import pandas as pd
from pymongo import MongoClient

logger = logging.getLogger(__name__)


def extract_reviews(
    host: str = "localhost",
    port: int = 27017,
    db_name: str = "product_reviews",
    last_extracted_date: Optional[datetime] = None,
) -> pd.DataFrame:
    """
    Extract product reviews from MongoDB.

    Parameters
    ----------
    host : str
        MongoDB host.
    port : int
        MongoDB port.
    db_name : str
        Database name containing the product_reviews collection.
    last_extracted_date : datetime, optional
        If provided, only extracts reviews with review_date > this value
        (incremental). If None, does a full extraction.

    Returns
    -------
    pd.DataFrame
        Flattened DataFrame with review columns.
    """
    client = MongoClient(host, port)
    db = client[db_name]
    collection = db["product_reviews"]

    # Build query filter
    query_filter = {}
    if last_extracted_date:
        # review_date is stored as ISO string, so compare as string
        query_filter["review_date"] = {"$gt": last_extracted_date.isoformat()}
        logger.info(f"Incremental extract: reviews after {last_extracted_date}")
    else:
        logger.info("Full extract: all reviews")

    # Fetch documents
    cursor = collection.find(query_filter)
    documents = list(cursor)
    client.close()

    if not documents:
        logger.warning("No review documents found.")
        return pd.DataFrame()

    # Flatten into DataFrame
    df = pd.DataFrame(documents)

    # Handle MongoDB _id (ObjectId → string)
    if "_id" in df.columns:
        df["_id"] = df["_id"].astype(str)
        df = df.drop(columns=["_id"])

    # Parse review_date to datetime
    if "review_date" in df.columns:
        df["review_date"] = pd.to_datetime(df["review_date"], errors="coerce")

    logger.info(f"Extracted {len(df):,} review documents from MongoDB")
    return df

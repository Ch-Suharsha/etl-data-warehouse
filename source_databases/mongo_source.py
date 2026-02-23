"""
MongoDB Source — Product Reviews Collection Seeder

Generates and seeds 80,000 product review documents into the MongoDB
source database with realistic rating distributions.
"""

import uuid
import random
import logging
from datetime import datetime, timedelta

from faker import Faker
from pymongo import MongoClient
from pymongo.errors import BulkWriteError

logger = logging.getLogger(__name__)
fake = Faker()
Faker.seed(42)
random.seed(42)

# -------------------------------------------------------------------
# Distribution constants
# -------------------------------------------------------------------
PRODUCT_CATEGORIES = ["Electronics", "Clothing", "Home", "Books", "Sports"]

# Skewed towards 4–5 stars
RATING_WEIGHTS = [0.05, 0.08, 0.12, 0.30, 0.45]  # 1–5 stars

# -------------------------------------------------------------------
# Generator
# -------------------------------------------------------------------

def generate_reviews(num_records: int = 80_000):
    """Yield review documents one at a time."""
    end_date = datetime.now()
    start_date = end_date - timedelta(days=365)

    for _ in range(num_records):
        rating = random.choices([1, 2, 3, 4, 5], weights=RATING_WEIGHTS, k=1)[0]

        yield {
            "review_id": str(uuid.uuid4()),
            "product_id": f"PROD_{random.randint(1, 500):04d}",
            "customer_id": f"CUST_{random.randint(1, 20000):05d}",
            "rating": rating,
            "review_text": fake.paragraph(nb_sentences=random.randint(1, 5)),
            "review_date": fake.date_time_between(
                start_date=start_date, end_date=end_date
            ).isoformat(),
            "verified_purchase": random.random() < 0.70,  # 70% verified
            "helpful_votes": random.randint(0, 50),
            "product_category": random.choice(PRODUCT_CATEGORIES),
        }


# -------------------------------------------------------------------
# Seeder
# -------------------------------------------------------------------

def seed_mongo(
    host: str = "localhost",
    port: int = 27017,
    db_name: str = "product_reviews",
    num_records: int = 80_000,
    batch_size: int = 5000,
):
    """
    Insert `num_records` review documents into MongoDB.

    Parameters
    ----------
    host : str
        MongoDB host.
    port : int
        MongoDB port.
    db_name : str
        Target database name.
    """
    client = MongoClient(host, port)
    db = client[db_name]
    collection = db["product_reviews"]

    # Create index on review_id for dedup
    collection.create_index("review_id", unique=True)
    logger.info("Ensured unique index on review_id.")

    inserted = 0
    batch: list[dict] = []

    for review in generate_reviews(num_records):
        batch.append(review)
        if len(batch) >= batch_size:
            inserted += _insert_batch(collection, batch)
            logger.info(f"Inserted {inserted:,}/{num_records:,} reviews")
            batch = []

    if batch:
        inserted += _insert_batch(collection, batch)

    logger.info(f"✓ MongoDB seeding complete — {inserted:,} reviews inserted.")
    client.close()
    return inserted


def _insert_batch(collection, batch: list[dict]) -> int:
    """Bulk-insert and return the count of successfully inserted docs."""
    try:
        result = collection.insert_many(batch, ordered=False)
        return len(result.inserted_ids)
    except BulkWriteError as e:
        # Some docs may be duplicates; count the successful ones
        return e.details.get("nInserted", 0)


# -------------------------------------------------------------------
# Standalone run
# -------------------------------------------------------------------

if __name__ == "__main__":
    import os
    from dotenv import load_dotenv

    load_dotenv()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s")

    seed_mongo(
        host=os.getenv("MONGO_SOURCE_HOST", "localhost"),
        port=int(os.getenv("MONGO_SOURCE_PORT", "27017")),
        db_name=os.getenv("MONGO_SOURCE_DB", "product_reviews"),
    )

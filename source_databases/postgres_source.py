"""
PostgreSQL Source — Orders Database Seeder

Generates and seeds 100,000 realistic e-commerce order records into
the PostgreSQL source database. Uses Faker for realistic data with
controlled distributions for status and payment methods.
"""

import uuid
import random
import logging
from datetime import datetime, timedelta

from faker import Faker
from sqlalchemy import create_engine, text

logger = logging.getLogger(__name__)
fake = Faker()
Faker.seed(42)
random.seed(42)

# -------------------------------------------------------------------
# Schema
# -------------------------------------------------------------------
CREATE_ORDERS_TABLE = """
CREATE TABLE IF NOT EXISTS orders (
    order_id        VARCHAR(36)     PRIMARY KEY,
    customer_id     VARCHAR(20)     NOT NULL,
    product_id      VARCHAR(20)     NOT NULL,
    order_date      TIMESTAMP       NOT NULL,
    quantity        INTEGER         NOT NULL,
    unit_price      DECIMAL(10,2)   NOT NULL,
    total_amount    DECIMAL(12,2)   NOT NULL,
    status          VARCHAR(20)     NOT NULL,
    payment_method  VARCHAR(30)     NOT NULL,
    shipping_address TEXT
);
"""

# -------------------------------------------------------------------
# Distribution constants
# -------------------------------------------------------------------
STATUS_WEIGHTS = {
    "COMPLETED": 0.80,
    "PENDING":   0.10,
    "CANCELLED": 0.05,
    "REFUNDED":  0.05,
}

PAYMENT_METHODS = ["CREDIT_CARD", "DEBIT_CARD", "PAYPAL", "BANK_TRANSFER"]

# -------------------------------------------------------------------
# Generator
# -------------------------------------------------------------------

def _random_status() -> str:
    """Return a status string based on realistic distribution."""
    return random.choices(
        list(STATUS_WEIGHTS.keys()),
        weights=list(STATUS_WEIGHTS.values()),
        k=1,
    )[0]


def generate_orders(num_records: int = 100_000):
    """Yield order dicts one at a time to keep memory low."""
    end_date = datetime.now()
    start_date = end_date - timedelta(days=365)

    for _ in range(num_records):
        quantity = random.randint(1, 20)
        unit_price = round(random.uniform(5.00, 500.00), 2)
        total_amount = round(quantity * unit_price, 2)

        yield {
            "order_id": str(uuid.uuid4()),
            "customer_id": f"CUST_{random.randint(1, 20000):05d}",
            "product_id": f"PROD_{random.randint(1, 500):04d}",
            "order_date": fake.date_time_between(
                start_date=start_date, end_date=end_date
            ),
            "quantity": quantity,
            "unit_price": unit_price,
            "total_amount": total_amount,
            "status": _random_status(),
            "payment_method": random.choice(PAYMENT_METHODS),
            "shipping_address": fake.address().replace("\n", ", "),
        }


# -------------------------------------------------------------------
# Seeder
# -------------------------------------------------------------------

def seed_postgres(connection_url: str, num_records: int = 100_000, batch_size: int = 5000):
    """
    Create the orders table and insert `num_records` rows in batches.

    Parameters
    ----------
    connection_url : str
        SQLAlchemy connection string, e.g.
        ``postgresql+psycopg2://source_user:source_pass@localhost:5433/ecommerce_orders``
    num_records : int
        Total number of order rows to generate.
    batch_size : int
        Rows per INSERT batch.
    """
    engine = create_engine(connection_url)

    with engine.begin() as conn:
        conn.execute(text(CREATE_ORDERS_TABLE))
        logger.info("Created orders table (if not exists).")

    inserted = 0
    batch = []

    for order in generate_orders(num_records):
        batch.append(order)
        if len(batch) >= batch_size:
            _insert_batch(engine, batch)
            inserted += len(batch)
            logger.info(f"Inserted {inserted:,}/{num_records:,} orders")
            batch = []

    # Remaining rows
    if batch:
        _insert_batch(engine, batch)
        inserted += len(batch)

    logger.info(f"✓ PostgreSQL seeding complete — {inserted:,} orders inserted.")
    return inserted


def _insert_batch(engine, batch: list[dict]):
    """Bulk-insert a batch of order dicts."""
    insert_sql = text("""
        INSERT INTO orders
            (order_id, customer_id, product_id, order_date, quantity,
             unit_price, total_amount, status, payment_method, shipping_address)
        VALUES
            (:order_id, :customer_id, :product_id, :order_date, :quantity,
             :unit_price, :total_amount, :status, :payment_method, :shipping_address)
        ON CONFLICT (order_id) DO NOTHING
    """)
    with engine.begin() as conn:
        conn.execute(insert_sql, batch)


# -------------------------------------------------------------------
# Standalone run
# -------------------------------------------------------------------

if __name__ == "__main__":
    import os
    from dotenv import load_dotenv

    load_dotenv()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s")

    pg_url = (
        f"postgresql+psycopg2://{os.getenv('PG_SOURCE_USER')}:{os.getenv('PG_SOURCE_PASSWORD')}"
        f"@{os.getenv('PG_SOURCE_HOST')}:{os.getenv('PG_SOURCE_PORT')}/{os.getenv('PG_SOURCE_DB')}"
    )
    seed_postgres(pg_url)

"""
MySQL Source — Customer Management Database Seeder

Generates and seeds 20,000 customer records into the MySQL source
database with realistic tier and activity distributions.
"""

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
CREATE_CUSTOMERS_TABLE = """
CREATE TABLE IF NOT EXISTS customers (
    customer_id     VARCHAR(20)     PRIMARY KEY,
    first_name      VARCHAR(50)     NOT NULL,
    last_name       VARCHAR(50)     NOT NULL,
    email           VARCHAR(100)    UNIQUE NOT NULL,
    phone           VARCHAR(30),
    city            VARCHAR(50),
    state           VARCHAR(50),
    country         VARCHAR(10)     DEFAULT 'US',
    signup_date     DATE            NOT NULL,
    customer_tier   VARCHAR(20)     NOT NULL,
    lifetime_value  DECIMAL(12,2)   NOT NULL,
    is_active       BOOLEAN         NOT NULL DEFAULT TRUE
);
"""

# -------------------------------------------------------------------
# Distribution constants
# -------------------------------------------------------------------
TIER_WEIGHTS = {
    "BRONZE":   0.50,
    "SILVER":   0.30,
    "GOLD":     0.15,
    "PLATINUM": 0.05,
}

TIER_LTV_RANGES = {
    "BRONZE":   (50.0, 500.0),
    "SILVER":   (500.0, 2000.0),
    "GOLD":     (2000.0, 10000.0),
    "PLATINUM": (10000.0, 50000.0),
}

US_STATES = [
    "CA", "TX", "FL", "NY", "PA", "IL", "OH", "GA", "NC", "MI",
    "NJ", "VA", "WA", "AZ", "MA", "TN", "IN", "MO", "MD", "WI",
    "CO", "MN", "SC", "AL", "LA", "KY", "OR", "OK", "CT", "UT",
]

# -------------------------------------------------------------------
# Generator
# -------------------------------------------------------------------

def _random_tier() -> str:
    return random.choices(
        list(TIER_WEIGHTS.keys()),
        weights=list(TIER_WEIGHTS.values()),
        k=1,
    )[0]


def generate_customers(num_records: int = 20_000):
    """Yield customer dicts."""
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=730)  # 2 years

    used_emails: set[str] = set()

    for i in range(1, num_records + 1):
        tier = _random_tier()
        ltv_lo, ltv_hi = TIER_LTV_RANGES[tier]

        # Unique email
        email = fake.unique.email()
        used_emails.add(email)

        is_active = random.random() < 0.85  # 85% active

        yield {
            "customer_id": f"CUST_{i:05d}",
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "email": email.lower(),
            "phone": fake.phone_number() if random.random() > 0.05 else None,
            "city": fake.city(),
            "state": random.choice(US_STATES),
            "country": "US",
            "signup_date": fake.date_between(start_date=start_date, end_date=end_date),
            "customer_tier": tier,
            "lifetime_value": round(random.uniform(ltv_lo, ltv_hi), 2),
            "is_active": is_active,
        }


# -------------------------------------------------------------------
# Seeder
# -------------------------------------------------------------------

def seed_mysql(connection_url: str, num_records: int = 20_000, batch_size: int = 5000):
    """
    Create the customers table and insert `num_records` rows.

    Parameters
    ----------
    connection_url : str
        SQLAlchemy connection string, e.g.
        ``mysql+pymysql://source_user:source_pass@localhost:3307/customer_management``
    """
    engine = create_engine(connection_url)

    with engine.begin() as conn:
        conn.execute(text(CREATE_CUSTOMERS_TABLE))
        logger.info("Created customers table (if not exists).")

    inserted = 0
    batch: list[dict] = []

    for customer in generate_customers(num_records):
        batch.append(customer)
        if len(batch) >= batch_size:
            _insert_batch(engine, batch)
            inserted += len(batch)
            logger.info(f"Inserted {inserted:,}/{num_records:,} customers")
            batch = []

    if batch:
        _insert_batch(engine, batch)
        inserted += len(batch)

    logger.info(f"✓ MySQL seeding complete — {inserted:,} customers inserted.")
    return inserted


def _insert_batch(engine, batch: list[dict]):
    """Bulk-insert a batch of customer dicts."""
    insert_sql = text("""
        INSERT IGNORE INTO customers
            (customer_id, first_name, last_name, email, phone,
             city, state, country, signup_date, customer_tier,
             lifetime_value, is_active)
        VALUES
            (:customer_id, :first_name, :last_name, :email, :phone,
             :city, :state, :country, :signup_date, :customer_tier,
             :lifetime_value, :is_active)
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

    mysql_url = (
        f"mysql+pymysql://{os.getenv('MYSQL_SOURCE_USER')}:{os.getenv('MYSQL_SOURCE_PASSWORD')}"
        f"@{os.getenv('MYSQL_SOURCE_HOST')}:{os.getenv('MYSQL_SOURCE_PORT')}/{os.getenv('MYSQL_SOURCE_DB')}"
    )
    seed_mysql(mysql_url)

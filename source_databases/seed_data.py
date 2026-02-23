"""
Master Seed Script — Seeds ALL Three Source Databases

Usage:
    python -m source_databases.seed_data

Connects to PostgreSQL (orders), MySQL (customers), and MongoDB
(product reviews), then generates and inserts sample data into each.
"""

import os
import sys
import logging
import time

from dotenv import load_dotenv

from source_databases.postgres_source import seed_postgres
from source_databases.mysql_source import seed_mysql
from source_databases.mongo_source import seed_mongo

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


def main():
    load_dotenv()

    results = {}
    start_total = time.time()

    # ---------------------------------------------------------------
    # 1. PostgreSQL — Orders (100,000 records)
    # ---------------------------------------------------------------
    logger.info("=" * 60)
    logger.info("SEEDING PostgreSQL — Orders")
    logger.info("=" * 60)

    pg_url = (
        f"postgresql+psycopg2://{os.getenv('PG_SOURCE_USER', 'source_user')}"
        f":{os.getenv('PG_SOURCE_PASSWORD', 'source_pass')}"
        f"@{os.getenv('PG_SOURCE_HOST', 'localhost')}"
        f":{os.getenv('PG_SOURCE_PORT', '5433')}"
        f"/{os.getenv('PG_SOURCE_DB', 'ecommerce_orders')}"
    )

    start = time.time()
    try:
        results["postgres"] = seed_postgres(pg_url, num_records=100_000)
    except Exception as e:
        logger.error(f"PostgreSQL seeding failed: {e}")
        results["postgres"] = 0
    logger.info(f"PostgreSQL seeding took {time.time() - start:.1f}s")

    # ---------------------------------------------------------------
    # 2. MySQL — Customers (20,000 records)
    # ---------------------------------------------------------------
    logger.info("=" * 60)
    logger.info("SEEDING MySQL — Customers")
    logger.info("=" * 60)

    mysql_url = (
        f"mysql+pymysql://{os.getenv('MYSQL_SOURCE_USER', 'source_user')}"
        f":{os.getenv('MYSQL_SOURCE_PASSWORD', 'source_pass')}"
        f"@{os.getenv('MYSQL_SOURCE_HOST', 'localhost')}"
        f":{os.getenv('MYSQL_SOURCE_PORT', '3307')}"
        f"/{os.getenv('MYSQL_SOURCE_DB', 'customer_management')}"
    )

    start = time.time()
    try:
        results["mysql"] = seed_mysql(mysql_url, num_records=20_000)
    except Exception as e:
        logger.error(f"MySQL seeding failed: {e}")
        results["mysql"] = 0
    logger.info(f"MySQL seeding took {time.time() - start:.1f}s")

    # ---------------------------------------------------------------
    # 3. MongoDB — Product Reviews (80,000 documents)
    # ---------------------------------------------------------------
    logger.info("=" * 60)
    logger.info("SEEDING MongoDB — Product Reviews")
    logger.info("=" * 60)

    start = time.time()
    try:
        results["mongodb"] = seed_mongo(
            host=os.getenv("MONGO_SOURCE_HOST", "localhost"),
            port=int(os.getenv("MONGO_SOURCE_PORT", "27017")),
            db_name=os.getenv("MONGO_SOURCE_DB", "product_reviews"),
            num_records=80_000,
        )
    except Exception as e:
        logger.error(f"MongoDB seeding failed: {e}")
        results["mongodb"] = 0
    logger.info(f"MongoDB seeding took {time.time() - start:.1f}s")

    # ---------------------------------------------------------------
    # Summary
    # ---------------------------------------------------------------
    elapsed = time.time() - start_total
    logger.info("=" * 60)
    logger.info("SEEDING SUMMARY")
    logger.info("=" * 60)
    logger.info(f"  PostgreSQL orders  : {results.get('postgres', 0):>10,}")
    logger.info(f"  MySQL customers    : {results.get('mysql', 0):>10,}")
    logger.info(f"  MongoDB reviews    : {results.get('mongodb', 0):>10,}")
    logger.info(f"  Total records      : {sum(results.values()):>10,}")
    logger.info(f"  Total time         : {elapsed:.1f}s")
    logger.info("=" * 60)

    # Non-zero exit if any source failed
    if any(v == 0 for v in results.values()):
        logger.warning("One or more sources failed to seed!")
        sys.exit(1)


if __name__ == "__main__":
    main()

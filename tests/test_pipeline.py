"""
Integration Tests for the ETL Pipeline

Tests the full pipeline flow:
  extract (mocked) → transform → validate → verify outputs

Also tests the loader functions with mocked database connections.
"""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime, date, timedelta
from unittest.mock import patch, MagicMock

from transformers.data_transformer import (
    clean_orders,
    clean_customers,
    clean_reviews,
    validate_referential_integrity,
)


# -------------------------------------------------------------------
# Fixtures — Realistic sample data
# -------------------------------------------------------------------

@pytest.fixture
def realistic_orders():
    """Generate a small realistic orders DataFrame."""
    np.random.seed(42)
    n = 500

    return pd.DataFrame({
        "order_id": [f"ord-{i:04d}" for i in range(n)],
        "customer_id": [f"CUST_{np.random.randint(1, 100):05d}" for _ in range(n)],
        "product_id": [f"PROD_{np.random.randint(1, 50):04d}" for _ in range(n)],
        "order_date": pd.date_range("2024-01-01", periods=n, freq="h"),
        "quantity": np.random.randint(1, 10, size=n),
        "unit_price": np.round(np.random.uniform(10, 300, size=n), 2),
        "total_amount": None,  # Will be calculated by transformer
        "status": np.random.choice(
            ["COMPLETED", "PENDING", "CANCELLED", "REFUNDED"],
            size=n, p=[0.8, 0.1, 0.05, 0.05],
        ),
        "payment_method": np.random.choice(
            ["CREDIT_CARD", "DEBIT_CARD", "PAYPAL", "BANK_TRANSFER"], size=n
        ),
        "shipping_address": [f"{i} Main St, City, ST" for i in range(n)],
    })


@pytest.fixture
def realistic_customers():
    """Generate a small realistic customers DataFrame."""
    n = 100

    return pd.DataFrame({
        "customer_id": [f"CUST_{i:05d}" for i in range(1, n + 1)],
        "first_name": [f"First_{i}" for i in range(n)],
        "last_name": [f"Last_{i}" for i in range(n)],
        "email": [f"user_{i}@example.com" for i in range(n)],
        "phone": [f"555-{i:04d}" if i % 10 != 0 else None for i in range(n)],
        "city": ["New York"] * n,
        "state": ["NY"] * n,
        "country": ["US"] * n,
        "signup_date": pd.date_range("2023-01-01", periods=n, freq="7D"),
        "customer_tier": np.random.choice(
            ["BRONZE", "SILVER", "GOLD", "PLATINUM"],
            size=n, p=[0.5, 0.3, 0.15, 0.05],
        ),
        "lifetime_value": np.round(np.random.uniform(100, 10000, size=n), 2),
        "is_active": [i % 7 != 0 for i in range(n)],
    })


@pytest.fixture
def realistic_reviews():
    """Generate a small realistic reviews DataFrame."""
    n = 200

    return pd.DataFrame({
        "review_id": [f"rev-{i:04d}" for i in range(n)],
        "product_id": [f"PROD_{np.random.randint(1, 50):04d}" for _ in range(n)],
        "customer_id": [f"CUST_{np.random.randint(1, 100):05d}" for _ in range(n)],
        "rating": np.random.choice([1, 2, 3, 4, 5], size=n, p=[0.05, 0.08, 0.12, 0.30, 0.45]),
        "review_text": [f"Review text {i}" for i in range(n)],
        "review_date": pd.date_range("2024-01-01", periods=n, freq="4h"),
        "verified_purchase": np.random.choice([True, False], size=n, p=[0.7, 0.3]),
        "helpful_votes": np.random.randint(0, 30, size=n),
        "product_category": np.random.choice(
            ["Electronics", "Clothing", "Home", "Books", "Sports"], size=n
        ),
    })


# -------------------------------------------------------------------
# Integration: Full Pipeline Flow
# -------------------------------------------------------------------

class TestFullPipeline:
    """Integration test: extract → transform → validate → verify outputs."""

    def test_pipeline_produces_valid_output(
        self, realistic_orders, realistic_customers, realistic_reviews
    ):
        """Full pipeline should produce clean, referentially valid data."""
        # Transform
        orders_clean = clean_orders(realistic_orders)
        customers_clean = clean_customers(realistic_customers)
        reviews_clean = clean_reviews(realistic_reviews)

        # Validate
        orders_valid = validate_referential_integrity(orders_clean, customers_clean)

        # --- Verify Orders ---
        assert orders_clean["order_id"].is_unique
        assert orders_clean["quantity"].isna().sum() == 0
        assert orders_clean["total_amount"].isna().sum() == 0
        assert all(s == s.upper() for s in orders_clean["status"])
        assert "order_month" in orders_clean.columns

        # --- Verify Customers ---
        assert customers_clean["customer_id"].is_unique
        assert all(e == e.lower() for e in customers_clean["email"])
        assert customers_clean["phone"].isna().sum() == 0
        assert "account_age_days" in customers_clean.columns

        # --- Verify Reviews ---
        assert reviews_clean["review_id"].is_unique
        assert reviews_clean["rating"].between(1, 5).all()
        assert "sentiment_category" in reviews_clean.columns

        # --- Verify Referential Integrity ---
        valid_ids = set(customers_clean["customer_id"])
        assert all(cid in valid_ids for cid in orders_valid["customer_id"])

    def test_record_counts_match_expectations(
        self, realistic_orders, realistic_customers, realistic_reviews
    ):
        """Record counts should be <= input (dedup may reduce)."""
        orders_clean = clean_orders(realistic_orders)
        customers_clean = clean_customers(realistic_customers)
        reviews_clean = clean_reviews(realistic_reviews)

        assert len(orders_clean) <= len(realistic_orders)
        assert len(customers_clean) <= len(realistic_customers)
        assert len(reviews_clean) <= len(realistic_reviews)

    def test_star_schema_relationships(
        self, realistic_orders, realistic_customers, realistic_reviews
    ):
        """Verify FK columns exist for star schema join."""
        orders_clean = clean_orders(realistic_orders)
        customers_clean = clean_customers(realistic_customers)

        # Fact has FK columns
        assert "customer_id" in orders_clean.columns
        assert "product_id" in orders_clean.columns

        # Dimension has business keys
        assert "customer_id" in customers_clean.columns


# -------------------------------------------------------------------
# Loader Function Tests (mocked DB)
# -------------------------------------------------------------------

class TestLoaderFunctions:
    """Test loader functions with mocked database."""

    @patch("loaders.warehouse_loader.create_engine")
    def test_populate_date_dimension(self, mock_engine):
        """Date dimension should generate correct number of records."""
        mock_conn = MagicMock()
        mock_engine.return_value.begin.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_engine.return_value.begin.return_value.__exit__ = MagicMock(return_value=False)

        from loaders.warehouse_loader import populate_date_dimension

        start = date(2024, 1, 1)
        end = date(2024, 1, 31)
        count = populate_date_dimension(start, end, "postgresql://test")

        assert count == 31  # 31 days in January

    @patch("loaders.warehouse_loader.create_engine")
    def test_log_etl_run(self, mock_engine):
        """ETL run logging should execute without error."""
        mock_conn = MagicMock()
        mock_engine.return_value.begin.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_engine.return_value.begin.return_value.__exit__ = MagicMock(return_value=False)

        from loaders.warehouse_loader import log_etl_run

        run_details = {
            "dag_id": "test_dag",
            "source_name": "test_source",
            "records_extracted": 100,
            "records_transformed": 95,
            "records_loaded": 90,
            "records_rejected": 5,
            "status": "SUCCESS",
        }

        # Should not raise
        log_etl_run(run_details, "postgresql://test")
        mock_conn.execute.assert_called_once()

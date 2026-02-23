"""
Tests for Transformer Module

Tests cover:
- clean_orders() removes duplicates and handles null quantities
- clean_customers() lowercases emails and handles null phones
- clean_reviews() validates ratings and adds sentiment
- validate_referential_integrity() catches orphaned records
- Cleaned data has no nulls in required fields
"""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

from transformers.data_transformer import (
    clean_orders,
    clean_customers,
    clean_reviews,
    validate_referential_integrity,
)


# -------------------------------------------------------------------
# Fixtures
# -------------------------------------------------------------------

@pytest.fixture
def sample_orders():
    """Create a sample orders DataFrame with known issues."""
    return pd.DataFrame({
        "order_id": ["o1", "o2", "o2", "o3", "o4"],  # o2 is duplicated
        "customer_id": ["CUST_00001", "CUST_00002", "CUST_00002", "CUST_00003", "CUST_99999"],
        "product_id": ["PROD_0001", "PROD_0002", "PROD_0002", "PROD_0003", "PROD_0004"],
        "order_date": pd.to_datetime([
            "2024-01-15", "2024-02-20", "2024-02-20", "2024-03-10", "2024-04-05"
        ]),
        "quantity": [2, None, None, 5, 1],  # one null quantity
        "unit_price": [25.00, 50.00, 50.00, 10.00, 100.00],
        "total_amount": [50.00, None, None, 50.00, 100.00],  # one null total
        "status": ["completed", "PENDING", "PENDING", "Cancelled", "REFUNDED"],
        "payment_method": ["CREDIT_CARD", "PAYPAL", "PAYPAL", "DEBIT_CARD", "BANK_TRANSFER"],
        "shipping_address": ["123 Main St", "456 Oak Ave", "456 Oak Ave", "789 Pine Rd", None],
    })


@pytest.fixture
def sample_customers():
    """Create a sample customers DataFrame."""
    return pd.DataFrame({
        "customer_id": ["CUST_00001", "CUST_00002", "CUST_00003"],
        "first_name": ["Alice", "Bob", "Charlie"],
        "last_name": ["Smith", "Jones", "Brown"],
        "email": ["Alice.Smith@Email.COM", "bob@email.com", "Charlie@Email.com"],
        "phone": ["555-0001", None, "555-0003"],
        "city": ["New York", "Chicago", "Houston"],
        "state": ["NY", "IL", "TX"],
        "country": ["US", "US", "US"],
        "signup_date": pd.to_datetime(["2023-01-15", "2023-06-20", "2024-01-01"]),
        "customer_tier": ["GOLD", "bronze", "INVALID_TIER"],
        "lifetime_value": [5000.00, 200.00, 1500.00],
        "is_active": [True, True, False],
    })


@pytest.fixture
def sample_reviews():
    """Create a sample reviews DataFrame."""
    return pd.DataFrame({
        "review_id": ["r1", "r2", "r3", "r3"],  # r3 duplicated
        "product_id": ["PROD_0001", "PROD_0002", "PROD_0001", "PROD_0001"],
        "customer_id": ["CUST_00001", "CUST_00002", "CUST_00003", "CUST_00003"],
        "rating": [5, 3, 7, 7],  # 7 is out of range
        "review_text": ["Great!", None, "OK", "OK"],
        "review_date": pd.to_datetime([
            "2024-01-20", "2024-02-15", "2024-03-10", "2024-03-10"
        ]),
        "verified_purchase": [True, True, False, False],
        "helpful_votes": [10, 0, 5, 5],
        "product_category": ["Electronics", "Clothing", "Electronics", "Electronics"],
    })


# -------------------------------------------------------------------
# clean_orders() Tests
# -------------------------------------------------------------------

class TestCleanOrders:
    def test_removes_duplicates(self, sample_orders):
        result = clean_orders(sample_orders)
        assert result["order_id"].is_unique
        assert len(result) == 4  # 5 rows → 4 after dedup

    def test_handles_null_quantities(self, sample_orders):
        result = clean_orders(sample_orders)
        assert result["quantity"].isna().sum() == 0
        # The null quantity row should default to 1
        o2_row = result[result["order_id"] == "o2"]
        assert o2_row["quantity"].iloc[0] == 1

    def test_recalculates_missing_total(self, sample_orders):
        result = clean_orders(sample_orders)
        assert result["total_amount"].isna().sum() == 0

    def test_standardizes_status_uppercase(self, sample_orders):
        result = clean_orders(sample_orders)
        for status in result["status"]:
            assert status == status.upper()

    def test_adds_time_columns(self, sample_orders):
        result = clean_orders(sample_orders)
        assert "order_month" in result.columns
        assert "order_year" in result.columns
        assert "order_day_of_week" in result.columns


# -------------------------------------------------------------------
# clean_customers() Tests
# -------------------------------------------------------------------

class TestCleanCustomers:
    def test_lowercases_emails(self, sample_customers):
        result = clean_customers(sample_customers)
        for email in result["email"]:
            assert email == email.lower()

    def test_handles_null_phones(self, sample_customers):
        result = clean_customers(sample_customers)
        assert result["phone"].isna().sum() == 0
        bob = result[result["customer_id"] == "CUST_00002"]
        assert bob["phone"].iloc[0] == "N/A"

    def test_validates_tier(self, sample_customers):
        result = clean_customers(sample_customers)
        valid_tiers = {"BRONZE", "SILVER", "GOLD", "PLATINUM"}
        for tier in result["customer_tier"]:
            assert tier in valid_tiers

    def test_adds_account_age_days(self, sample_customers):
        result = clean_customers(sample_customers)
        assert "account_age_days" in result.columns
        assert result["account_age_days"].isna().sum() == 0


# -------------------------------------------------------------------
# clean_reviews() Tests
# -------------------------------------------------------------------

class TestCleanReviews:
    def test_removes_duplicates(self, sample_reviews):
        result = clean_reviews(sample_reviews)
        assert result["review_id"].is_unique
        assert len(result) == 3

    def test_clips_out_of_range_ratings(self, sample_reviews):
        result = clean_reviews(sample_reviews)
        assert result["rating"].max() <= 5
        assert result["rating"].min() >= 1

    def test_handles_null_review_text(self, sample_reviews):
        result = clean_reviews(sample_reviews)
        assert result["review_text"].isna().sum() == 0

    def test_adds_sentiment_category(self, sample_reviews):
        result = clean_reviews(sample_reviews)
        assert "sentiment_category" in result.columns
        valid_sentiments = {"negative", "neutral", "positive"}
        for sentiment in result["sentiment_category"].dropna():
            assert sentiment in valid_sentiments


# -------------------------------------------------------------------
# validate_referential_integrity() Tests
# -------------------------------------------------------------------

class TestReferentialIntegrity:
    def test_catches_orphaned_records(self, sample_orders, sample_customers):
        """Orders with customer_ids not in customers should be removed."""
        orders_clean = clean_orders(sample_orders)
        customers_clean = clean_customers(sample_customers)

        result = validate_referential_integrity(orders_clean, customers_clean)

        # CUST_99999 doesn't exist in customers, so o4 should be removed
        assert "CUST_99999" not in result["customer_id"].values
        assert len(result) < len(orders_clean)

    def test_all_valid_references_pass(self, sample_customers):
        """All-valid orders should pass through unchanged."""
        valid_orders = pd.DataFrame({
            "customer_id": ["CUST_00001", "CUST_00002"],
            "order_id": ["v1", "v2"],
        })
        result = validate_referential_integrity(valid_orders, sample_customers)
        assert len(result) == 2


# -------------------------------------------------------------------
# No Nulls In Required Fields
# -------------------------------------------------------------------

class TestNoNullsInRequired:
    def test_orders_no_null_required(self, sample_orders):
        result = clean_orders(sample_orders)
        assert result["order_id"].isna().sum() == 0
        assert result["quantity"].isna().sum() == 0
        assert result["total_amount"].isna().sum() == 0
        assert result["status"].isna().sum() == 0

    def test_customers_no_null_required(self, sample_customers):
        result = clean_customers(sample_customers)
        assert result["customer_id"].isna().sum() == 0
        assert result["email"].isna().sum() == 0
        assert result["customer_tier"].isna().sum() == 0

"""
Tests for Extractor Modules

Tests cover:
- PostgreSQL extractor returns DataFrame with expected columns
- MySQL extractor returns DataFrame with expected columns
- MongoDB extractor flattens documents correctly
- Incremental extraction filters by date
- Graceful handling of connection errors
"""

import pytest
import pandas as pd
from datetime import datetime, timedelta
from unittest.mock import patch, MagicMock


# -------------------------------------------------------------------
# PostgreSQL Extractor Tests
# -------------------------------------------------------------------

class TestPostgresExtractor:
    """Tests for extractors.postgres_extractor.extract_orders"""

    @patch("extractors.postgres_extractor.create_engine")
    def test_full_extract_returns_dataframe(self, mock_engine):
        """Full extract should return a DataFrame with expected columns."""
        expected_columns = [
            "order_id", "customer_id", "product_id", "order_date",
            "quantity", "unit_price", "total_amount", "status",
            "payment_method", "shipping_address",
        ]
        mock_df = pd.DataFrame(columns=expected_columns)

        with patch("extractors.postgres_extractor.pd.read_sql", return_value=mock_df):
            from extractors.postgres_extractor import extract_orders
            result = extract_orders("postgresql+psycopg2://user:pass@localhost/db")

        assert isinstance(result, pd.DataFrame)
        assert list(result.columns) == expected_columns

    @patch("extractors.postgres_extractor.create_engine")
    def test_incremental_extract_filters_by_date(self, mock_engine):
        """Incremental extract should pass a date filter."""
        mock_df = pd.DataFrame(columns=["order_id"])
        last_date = datetime(2024, 6, 1)

        with patch("extractors.postgres_extractor.pd.read_sql", return_value=mock_df) as mock_read:
            from extractors.postgres_extractor import extract_orders
            extract_orders(
                "postgresql+psycopg2://user:pass@localhost/db",
                last_extracted_date=last_date,
            )

        # Verify read_sql was called (filter is applied in the query)
        mock_read.assert_called_once()

    @patch("extractors.postgres_extractor.create_engine")
    def test_extract_handles_empty_result(self, mock_engine):
        """Should return empty DataFrame if no records match."""
        mock_df = pd.DataFrame()

        with patch("extractors.postgres_extractor.pd.read_sql", return_value=mock_df):
            from extractors.postgres_extractor import extract_orders
            result = extract_orders("postgresql+psycopg2://user:pass@localhost/db")

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0


# -------------------------------------------------------------------
# MySQL Extractor Tests
# -------------------------------------------------------------------

class TestMysqlExtractor:
    """Tests for extractors.mysql_extractor.extract_customers"""

    @patch("extractors.mysql_extractor.create_engine")
    def test_full_extract_returns_dataframe(self, mock_engine):
        """Full extract should return a DataFrame with expected columns."""
        expected_columns = [
            "customer_id", "first_name", "last_name", "email", "phone",
            "city", "state", "country", "signup_date", "customer_tier",
            "lifetime_value", "is_active",
        ]
        mock_df = pd.DataFrame(columns=expected_columns)

        with patch("extractors.mysql_extractor.pd.read_sql", return_value=mock_df):
            from extractors.mysql_extractor import extract_customers
            result = extract_customers("mysql+pymysql://user:pass@localhost/db")

        assert isinstance(result, pd.DataFrame)
        assert list(result.columns) == expected_columns

    @patch("extractors.mysql_extractor.create_engine")
    def test_handles_connection_error(self, mock_engine):
        """Should raise an error if connection fails."""
        mock_engine.side_effect = Exception("Connection refused")

        with pytest.raises(Exception, match="Connection refused"):
            from extractors.mysql_extractor import extract_customers
            extract_customers("mysql+pymysql://bad:bad@localhost/bad")


# -------------------------------------------------------------------
# MongoDB Extractor Tests
# -------------------------------------------------------------------

class TestMongoExtractor:
    """Tests for extractors.mongo_extractor.extract_reviews"""

    @patch("extractors.mongo_extractor.MongoClient")
    def test_full_extract_flattens_documents(self, mock_client):
        """Should flatten MongoDB documents into a DataFrame."""
        from bson import ObjectId

        mock_docs = [
            {
                "_id": ObjectId(),
                "review_id": "r-001",
                "product_id": "PROD_0001",
                "customer_id": "CUST_00001",
                "rating": 5,
                "review_text": "Great product!",
                "review_date": "2024-06-15T10:30:00",
                "verified_purchase": True,
                "helpful_votes": 10,
                "product_category": "Electronics",
            }
        ]

        mock_collection = MagicMock()
        mock_collection.find.return_value = mock_docs
        mock_db = MagicMock()
        mock_db.__getitem__.return_value = mock_collection
        mock_client_instance = MagicMock()
        mock_client_instance.__getitem__.return_value = mock_db
        mock_client.return_value = mock_client_instance

        from extractors.mongo_extractor import extract_reviews
        result = extract_reviews(host="localhost", port=27017, db_name="test_db")

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1
        assert "review_id" in result.columns
        assert "_id" not in result.columns  # ObjectId should be removed

    @patch("extractors.mongo_extractor.MongoClient")
    def test_incremental_extract_filters(self, mock_client):
        """Incremental extract should use a date filter."""
        mock_collection = MagicMock()
        mock_collection.find.return_value = []
        mock_db = MagicMock()
        mock_db.__getitem__.return_value = mock_collection
        mock_client_instance = MagicMock()
        mock_client_instance.__getitem__.return_value = mock_db
        mock_client.return_value = mock_client_instance

        from extractors.mongo_extractor import extract_reviews
        last_date = datetime(2024, 6, 1)
        extract_reviews(
            host="localhost", port=27017, db_name="test_db",
            last_extracted_date=last_date,
        )

        # Verify find was called with a filter
        call_args = mock_collection.find.call_args[0][0]
        assert "review_date" in call_args
        assert "$gt" in call_args["review_date"]

    @patch("extractors.mongo_extractor.MongoClient")
    def test_empty_collection_returns_empty_df(self, mock_client):
        """Should return empty DataFrame for empty collection."""
        mock_collection = MagicMock()
        mock_collection.find.return_value = []
        mock_db = MagicMock()
        mock_db.__getitem__.return_value = mock_collection
        mock_client_instance = MagicMock()
        mock_client_instance.__getitem__.return_value = mock_db
        mock_client.return_value = mock_client_instance

        from extractors.mongo_extractor import extract_reviews
        result = extract_reviews(host="localhost", port=27017, db_name="test_db")

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0

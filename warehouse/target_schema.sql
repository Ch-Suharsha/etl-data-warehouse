-- ============================================================
-- Star Schema DDL for the Analytics Warehouse
-- ============================================================
-- Target: PostgreSQL (analytics_warehouse)
-- This file is auto-loaded by Docker via:
--   /docker-entrypoint-initdb.d/schema.sql
-- ============================================================

-- ============================================================
-- DIMENSION TABLES
-- ============================================================

CREATE TABLE IF NOT EXISTS dim_customers (
    customer_key    SERIAL          PRIMARY KEY,
    customer_id     VARCHAR(20)     UNIQUE NOT NULL,
    first_name      VARCHAR(50),
    last_name       VARCHAR(50),
    email           VARCHAR(100),
    city            VARCHAR(50),
    state           VARCHAR(50),
    country         VARCHAR(10)     DEFAULT 'US',
    customer_tier   VARCHAR(20),
    lifetime_value  DECIMAL(12,2),
    is_active       BOOLEAN,
    account_age_days INTEGER,
    loaded_at       TIMESTAMP       DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP       DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS dim_products (
    product_key     SERIAL          PRIMARY KEY,
    product_id      VARCHAR(20)     UNIQUE NOT NULL,
    product_category VARCHAR(50),
    avg_rating      DECIMAL(3,2),
    total_reviews   INTEGER,
    loaded_at       TIMESTAMP       DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP       DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS dim_date (
    date_key        INTEGER         PRIMARY KEY,  -- YYYYMMDD
    full_date       DATE            NOT NULL,
    day_of_week     INTEGER,
    day_name        VARCHAR(10),
    month           INTEGER,
    month_name      VARCHAR(10),
    quarter         INTEGER,
    year            INTEGER,
    is_weekend      BOOLEAN
);


-- ============================================================
-- FACT TABLES
-- ============================================================

CREATE TABLE IF NOT EXISTS fact_orders (
    order_key       SERIAL          PRIMARY KEY,
    order_id        VARCHAR(36)     UNIQUE NOT NULL,
    customer_key    INTEGER         REFERENCES dim_customers(customer_key),
    product_key     INTEGER         REFERENCES dim_products(product_key),
    date_key        INTEGER         REFERENCES dim_date(date_key),
    quantity        INTEGER,
    unit_price      DECIMAL(10,2),
    total_amount    DECIMAL(12,2),
    status          VARCHAR(20),
    payment_method  VARCHAR(30),
    loaded_at       TIMESTAMP       DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS fact_daily_sales (
    date_key        INTEGER         REFERENCES dim_date(date_key),
    product_key     INTEGER         REFERENCES dim_products(product_key),
    total_revenue   DECIMAL(15,2),
    total_orders    INTEGER,
    avg_order_value DECIMAL(10,2),
    cancelled_orders INTEGER,
    refunded_amount DECIMAL(12,2),
    PRIMARY KEY (date_key, product_key)
);


-- ============================================================
-- ETL AUDIT TABLE
-- ============================================================

CREATE TABLE IF NOT EXISTS etl_run_log (
    run_id          SERIAL          PRIMARY KEY,
    dag_id          VARCHAR(50),
    run_date        TIMESTAMP       DEFAULT CURRENT_TIMESTAMP,
    source_name     VARCHAR(30),
    records_extracted   INTEGER,
    records_transformed INTEGER,
    records_loaded      INTEGER,
    records_rejected    INTEGER,
    status          VARCHAR(20),
    error_message   TEXT
);


-- ============================================================
-- INDEXES
-- ============================================================

CREATE INDEX IF NOT EXISTS idx_fact_orders_customer  ON fact_orders(customer_key);
CREATE INDEX IF NOT EXISTS idx_fact_orders_date      ON fact_orders(date_key);
CREATE INDEX IF NOT EXISTS idx_fact_orders_product   ON fact_orders(product_key);
CREATE INDEX IF NOT EXISTS idx_fact_orders_status    ON fact_orders(status);
CREATE INDEX IF NOT EXISTS idx_fact_daily_date       ON fact_daily_sales(date_key);
CREATE INDEX IF NOT EXISTS idx_dim_customers_tier    ON dim_customers(customer_tier);

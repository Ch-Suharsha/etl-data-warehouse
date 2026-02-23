-- ============================================================
-- Analytics Queries for the Star Schema Data Warehouse
-- ============================================================
-- These queries run against the analytics_warehouse and
-- demonstrate CTEs, window functions, and complex joins.
-- ============================================================


-- ============================================================
-- Query 1: Monthly Revenue Trend with Running Total
-- ============================================================
-- Shows monthly revenue, month-over-month change, and a running
-- cumulative total using LAG() and SUM() OVER.

WITH monthly_revenue AS (
    SELECT
        d.year,
        d.month,
        d.month_name,
        SUM(f.total_amount)             AS revenue,
        COUNT(*)                        AS order_count,
        AVG(f.total_amount)             AS avg_order_value
    FROM fact_orders f
    JOIN dim_date d ON f.date_key = d.date_key
    WHERE f.status = 'COMPLETED'
    GROUP BY d.year, d.month, d.month_name
    ORDER BY d.year, d.month
)

SELECT
    year,
    month,
    month_name,
    revenue,
    order_count,
    ROUND(avg_order_value, 2)                                       AS avg_order_value,
    LAG(revenue) OVER (ORDER BY year, month)                        AS prev_month_revenue,
    ROUND(
        (revenue - LAG(revenue) OVER (ORDER BY year, month))
        / NULLIF(LAG(revenue) OVER (ORDER BY year, month), 0) * 100,
        1
    )                                                                AS mom_change_pct,
    SUM(revenue) OVER (ORDER BY year, month
                       ROWS UNBOUNDED PRECEDING)                    AS running_total
FROM monthly_revenue;


-- ============================================================
-- Query 2: Customer Tier Analysis with Percentile Ranking
-- ============================================================
-- Ranks customers by total spend within their tier using NTILE()
-- and shows tier-level aggregates with AVG() OVER.

WITH customer_spend AS (
    SELECT
        c.customer_id,
        c.first_name || ' ' || c.last_name  AS customer_name,
        c.customer_tier,
        c.lifetime_value,
        SUM(f.total_amount)                  AS total_spend,
        COUNT(f.order_key)                   AS total_orders
    FROM dim_customers c
    JOIN fact_orders f ON c.customer_key = f.customer_key
    WHERE f.status IN ('COMPLETED', 'PENDING')
    GROUP BY c.customer_id, c.first_name, c.last_name,
             c.customer_tier, c.lifetime_value
)

SELECT
    customer_id,
    customer_name,
    customer_tier,
    total_spend,
    total_orders,
    NTILE(4) OVER (
        PARTITION BY customer_tier
        ORDER BY total_spend DESC
    )                                                              AS spend_quartile,
    ROUND(
        AVG(total_spend) OVER (PARTITION BY customer_tier), 2
    )                                                              AS tier_avg_spend,
    ROUND(total_spend - AVG(total_spend) OVER (PARTITION BY customer_tier), 2)
                                                                   AS vs_tier_avg
FROM customer_spend
ORDER BY customer_tier, total_spend DESC;


-- ============================================================
-- Query 3: Product Category Performance with Ranking
-- ============================================================
-- Ranks products by revenue within their category and shows
-- category-level totals using RANK() and window aggregates.

WITH product_performance AS (
    SELECT
        p.product_id,
        p.product_category,
        p.avg_rating,
        p.total_reviews,
        SUM(f.total_amount)    AS product_revenue,
        COUNT(f.order_key)     AS product_orders
    FROM dim_products p
    JOIN fact_orders f ON p.product_key = f.product_key
    WHERE f.status = 'COMPLETED'
    GROUP BY p.product_id, p.product_category, p.avg_rating, p.total_reviews
)

SELECT
    product_id,
    product_category,
    avg_rating,
    total_reviews,
    product_revenue,
    product_orders,
    RANK() OVER (
        PARTITION BY product_category
        ORDER BY product_revenue DESC
    )                                                               AS category_rank,
    ROUND(
        product_revenue * 100.0
        / SUM(product_revenue) OVER (PARTITION BY product_category),
        1
    )                                                               AS pct_of_category,
    SUM(product_revenue) OVER (PARTITION BY product_category)      AS category_total
FROM product_performance
ORDER BY product_category, category_rank;


-- ============================================================
-- Query 4: Customer Retention Cohort Analysis
-- ============================================================
-- Groups customers by signup month (cohort), tracks how many
-- placed orders in subsequent months, and shows retention rates.

WITH customer_cohorts AS (
    SELECT
        c.customer_id,
        DATE_TRUNC('month', c.signup_date::TIMESTAMP)   AS cohort_month,
        MIN(d.full_date)                                AS first_order_date
    FROM dim_customers c
    JOIN fact_orders f ON c.customer_key = f.customer_key
    JOIN dim_date d ON f.date_key = d.date_key
    GROUP BY c.customer_id, DATE_TRUNC('month', c.signup_date::TIMESTAMP)
),

monthly_activity AS (
    SELECT
        cc.cohort_month,
        DATE_TRUNC('month', d.full_date::TIMESTAMP)                     AS activity_month,
        COUNT(DISTINCT cc.customer_id)                                  AS active_customers,
        EXTRACT(MONTH FROM AGE(
            DATE_TRUNC('month', d.full_date::TIMESTAMP), cc.cohort_month
        ))::INTEGER
        + EXTRACT(YEAR FROM AGE(
            DATE_TRUNC('month', d.full_date::TIMESTAMP), cc.cohort_month
        ))::INTEGER * 12                                                AS months_since_signup
    FROM customer_cohorts cc
    JOIN fact_orders f ON cc.customer_id = (
        SELECT customer_id FROM dim_customers WHERE customer_key = f.customer_key
    )
    JOIN dim_date d ON f.date_key = d.date_key
    GROUP BY cc.cohort_month, DATE_TRUNC('month', d.full_date::TIMESTAMP)
),

cohort_sizes AS (
    SELECT
        cohort_month,
        COUNT(DISTINCT customer_id) AS cohort_size
    FROM customer_cohorts
    GROUP BY cohort_month
)

SELECT
    TO_CHAR(ma.cohort_month, 'YYYY-MM')     AS cohort,
    cs.cohort_size,
    ma.months_since_signup,
    ma.active_customers,
    ROUND(
        ma.active_customers * 100.0 / cs.cohort_size, 1
    )                                        AS retention_pct
FROM monthly_activity ma
JOIN cohort_sizes cs ON ma.cohort_month = cs.cohort_month
WHERE ma.months_since_signup BETWEEN 0 AND 6
ORDER BY ma.cohort_month, ma.months_since_signup;


-- ============================================================
-- Query 5: Daily Sales Anomaly Detection
-- ============================================================
-- Computes a 7-day moving average of daily revenue, then flags
-- days where revenue deviates >2 standard deviations from
-- that moving average.

WITH daily_revenue AS (
    SELECT
        d.full_date,
        d.day_name,
        d.is_weekend,
        SUM(f.total_amount)     AS daily_total,
        COUNT(f.order_key)      AS daily_orders
    FROM fact_orders f
    JOIN dim_date d ON f.date_key = d.date_key
    WHERE f.status IN ('COMPLETED', 'PENDING')
    GROUP BY d.full_date, d.day_name, d.is_weekend
),

moving_stats AS (
    SELECT
        full_date,
        day_name,
        is_weekend,
        daily_total,
        daily_orders,
        ROUND(
            AVG(daily_total) OVER (
                ORDER BY full_date
                ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
            ), 2
        )                                                       AS moving_avg_7d,
        ROUND(
            STDDEV(daily_total) OVER (
                ORDER BY full_date
                ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
            ), 2
        )                                                       AS moving_stddev_7d
    FROM daily_revenue
)

SELECT
    full_date,
    day_name,
    is_weekend,
    daily_total,
    daily_orders,
    moving_avg_7d,
    moving_stddev_7d,
    ROUND(daily_total - moving_avg_7d, 2)                       AS deviation,
    CASE
        WHEN moving_stddev_7d > 0
             AND ABS(daily_total - moving_avg_7d) > 2 * moving_stddev_7d
        THEN 'ANOMALY'
        ELSE 'NORMAL'
    END                                                          AS anomaly_flag
FROM moving_stats
WHERE moving_avg_7d IS NOT NULL
ORDER BY full_date;

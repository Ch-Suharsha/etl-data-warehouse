-- fact_daily_sales.sql
-- Mart table: Aggregated daily sales per product

WITH daily_agg AS (
    SELECT
        d.date_key,
        p.product_key,
        SUM(o.total_amount) AS total_revenue,
        COUNT(*) AS total_orders,
        AVG(o.total_amount) AS avg_order_value,
        SUM(CASE WHEN o.status = 'CANCELLED' THEN 1 ELSE 0 END) AS cancelled_orders,
        SUM(CASE WHEN o.status = 'REFUNDED' THEN o.total_amount ELSE 0 END) AS refunded_amount
    FROM {{ ref('stg_orders') }} o
    LEFT JOIN {{ ref('dim_products') }} p
        ON o.product_id = p.product_id
    LEFT JOIN dim_date d
        ON DATE(o.order_date) = d.full_date
    WHERE d.date_key IS NOT NULL
      AND p.product_key IS NOT NULL
    GROUP BY d.date_key, p.product_key
)

SELECT
    date_key,
    product_key,
    ROUND(total_revenue::NUMERIC, 2) AS total_revenue,
    total_orders,
    ROUND(avg_order_value::NUMERIC, 2) AS avg_order_value,
    cancelled_orders,
    ROUND(refunded_amount::NUMERIC, 2) AS refunded_amount
FROM daily_agg

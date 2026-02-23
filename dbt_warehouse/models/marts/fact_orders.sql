-- fact_orders.sql
-- Mart table: Order fact with resolved surrogate keys

SELECT
    o.order_id,
    c.customer_key,
    p.product_key,
    d.date_key,
    o.quantity,
    o.unit_price,
    o.total_amount,
    o.status,
    o.payment_method
FROM {{ ref('stg_orders') }} o
LEFT JOIN {{ ref('dim_customers') }} c
    ON o.customer_id = c.customer_id
LEFT JOIN {{ ref('dim_products') }} p
    ON o.product_id = p.product_id
LEFT JOIN dim_date d
    ON DATE(o.order_date) = d.full_date

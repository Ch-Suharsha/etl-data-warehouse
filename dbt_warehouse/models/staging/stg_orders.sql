-- stg_orders.sql
-- Staging view: clean and enrich raw orders

SELECT
    order_id,
    customer_id,
    product_id,
    order_date,
    quantity,
    unit_price,
    quantity * unit_price AS calculated_total,
    COALESCE(total_amount, quantity * unit_price) AS total_amount,
    UPPER(status) AS status,
    payment_method,
    EXTRACT(MONTH FROM order_date) AS order_month,
    EXTRACT(YEAR FROM order_date) AS order_year,
    EXTRACT(DOW FROM order_date) AS day_of_week
FROM {{ source('raw', 'raw_orders') }}
WHERE order_id IS NOT NULL

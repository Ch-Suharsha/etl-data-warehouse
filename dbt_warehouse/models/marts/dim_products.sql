-- dim_products.sql
-- Mart table: Product dimension derived from review aggregates

SELECT
    ROW_NUMBER() OVER (ORDER BY product_id) AS product_key,
    product_id,
    product_category,
    avg_rating,
    total_reviews
FROM {{ ref('stg_products') }}

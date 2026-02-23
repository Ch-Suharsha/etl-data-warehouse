-- stg_products.sql
-- Staging view: derive product dimensions from reviews

WITH review_stats AS (
    SELECT
        product_id,
        product_category,
        AVG(rating) AS avg_rating,
        COUNT(*) AS total_reviews
    FROM {{ source('raw', 'raw_reviews') }}
    WHERE product_id IS NOT NULL
    GROUP BY product_id, product_category
)

SELECT
    product_id,
    product_category,
    ROUND(avg_rating::NUMERIC, 2) AS avg_rating,
    total_reviews
FROM review_stats

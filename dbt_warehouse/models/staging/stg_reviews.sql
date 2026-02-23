-- stg_reviews.sql
-- Staging view: clean and enrich raw product reviews

SELECT
    review_id,
    product_id,
    customer_id,
    rating,
    COALESCE(review_text, '') AS review_text,
    review_date::TIMESTAMP AS review_date,
    verified_purchase,
    helpful_votes,
    product_category,
    CASE
        WHEN rating <= 2 THEN 'negative'
        WHEN rating = 3  THEN 'neutral'
        WHEN rating >= 4 THEN 'positive'
    END AS sentiment_category
FROM {{ source('raw', 'raw_reviews') }}
WHERE review_id IS NOT NULL

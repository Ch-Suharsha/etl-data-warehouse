-- dim_customers.sql
-- Mart table: Customer dimension with surrogate keys

SELECT
    ROW_NUMBER() OVER (ORDER BY customer_id) AS customer_key,
    customer_id,
    first_name,
    last_name,
    email,
    city,
    state,
    country,
    customer_tier,
    lifetime_value,
    is_active,
    account_age_days
FROM {{ ref('stg_customers') }}

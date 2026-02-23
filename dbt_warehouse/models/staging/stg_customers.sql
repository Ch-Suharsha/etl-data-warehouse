-- stg_customers.sql
-- Staging view: clean and enrich raw customers

SELECT
    customer_id,
    first_name,
    last_name,
    LOWER(email) AS email,
    COALESCE(phone, 'N/A') AS phone,
    city,
    state,
    country,
    UPPER(customer_tier) AS customer_tier,
    lifetime_value,
    is_active,
    signup_date,
    CURRENT_DATE - signup_date AS account_age_days
FROM {{ source('raw', 'raw_customers') }}
WHERE customer_id IS NOT NULL

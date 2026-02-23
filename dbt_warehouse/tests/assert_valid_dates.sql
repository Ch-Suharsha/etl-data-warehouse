-- assert_valid_dates.sql
-- Custom dbt test: ensure all fact_orders have valid date references
-- If this returns any rows, the test FAILS.

SELECT *
FROM {{ ref('fact_orders') }}
WHERE date_key IS NULL
   OR date_key < 20230101

-- assert_positive_amounts.sql
-- Custom dbt test: ensure no negative total_amount in fact_orders
-- If this returns any rows, the test FAILS.

SELECT *
FROM {{ ref('fact_orders') }}
WHERE total_amount < 0

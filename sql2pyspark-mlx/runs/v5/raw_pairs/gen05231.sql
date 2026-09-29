WITH filtered_customers AS (
    SELECT *
    FROM customers
    WHERE signup_date >= '2023-09-07' AND (credit_limit BETWEEN 6600 AND 15800 OR is_active)
),
customers_stats AS (
    SELECT is_active, MIN(credit_limit) AS min_credit_limit
    FROM filtered_customers
    GROUP BY is_active
)
SELECT *
FROM customers_stats;

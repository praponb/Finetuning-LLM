WITH filtered_customers AS (
    SELECT *
    FROM customers
    WHERE email LIKE '%@gmail.com' AND credit_limit < 8300 AND segment IS NOT NULL
),
customers_stats AS (
    SELECT segment, SUM(age) AS total_age, ROUND((100 * SUM(CASE WHEN city = 'New York' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_new_york
    FROM filtered_customers
    GROUP BY segment
)
SELECT *
FROM customers_stats
WHERE pct_new_york < 79
ORDER BY segment

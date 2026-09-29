WITH base AS (
    SELECT *
    FROM customers
    WHERE segment IN ('SMB', 'Consumer', 'Corporate')
),
customers_stats AS (
    SELECT city, ROUND((100 * SUM(CASE WHEN segment = 'Corporate' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_corporate, MAX(credit_limit) AS max_credit_limit
    FROM base
    GROUP BY city
)
SELECT *
FROM customers_stats
ORDER BY pct_corporate DESC;

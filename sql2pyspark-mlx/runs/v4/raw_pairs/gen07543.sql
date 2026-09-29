WITH recent_customers AS (
    SELECT *
    FROM customers
    WHERE segment IN ('SMB', 'Corporate', 'Consumer')
),
customers_stats AS (
    SELECT city, MIN(age) min_age, AVG(age) avg_age
    FROM recent_customers
    GROUP BY city
)
SELECT *
FROM customers_stats
WHERE avg_age < 61
ORDER BY city;

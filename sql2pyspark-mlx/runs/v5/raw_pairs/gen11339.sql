WITH recent_customers AS (
    SELECT *
    FROM customers
    WHERE email LIKE '%@mail.co.th'
),
customers_stats AS (
    SELECT is_active, ROUND(STDDEV(age), 2) AS stddev_age
    FROM recent_customers
    GROUP BY is_active
)
SELECT is_active, stddev_age
FROM customers_stats;

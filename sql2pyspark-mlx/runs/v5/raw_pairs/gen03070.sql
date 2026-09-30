WITH recent_customers AS (
    SELECT *
    FROM customers
    WHERE credit_limit < 13100
),
customers_stats AS (
    SELECT DATE_FORMAT(signup_date, 'yyyy-MM') AS signup_month, city, ROUND(STDDEV(age), 2) AS stddev_age
    FROM recent_customers
    GROUP BY DATE_FORMAT(signup_date, 'yyyy-MM'), city
)
SELECT signup_month, stddev_age
FROM customers_stats
WHERE stddev_age > 45
ORDER BY stddev_age DESC, signup_month DESC

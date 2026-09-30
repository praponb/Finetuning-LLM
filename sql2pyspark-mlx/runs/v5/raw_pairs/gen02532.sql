WITH ValidCustomers AS (
    SELECT *
    FROM customers
    WHERE email IS NULL
),
country_summary AS (
    SELECT country, is_active, SUM(age) AS total_age
    FROM ValidCustomers
    GROUP BY country, is_active
)
SELECT *
FROM country_summary
ORDER BY total_age ASC

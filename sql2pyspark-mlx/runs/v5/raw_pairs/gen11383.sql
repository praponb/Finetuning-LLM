SELECT
    country,
    is_active,
    MAX(signup_date) AS latest_signup_date,
    ROUND(AVG(age), 2) AS avg_age,
    COUNT(age) AS age_count
FROM customers
GROUP BY country, is_active
HAVING COUNT(age) > 3

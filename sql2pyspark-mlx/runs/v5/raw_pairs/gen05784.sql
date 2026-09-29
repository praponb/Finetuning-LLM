SELECT
    city,
    segment,
    MAX(age) - MIN(age) AS age_range,
    MAX(signup_date) AS last_signup_date
FROM customers
GROUP BY city, segment
ORDER BY city DESC;

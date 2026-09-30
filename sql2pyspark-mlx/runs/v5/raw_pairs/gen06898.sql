SELECT
    segment,
    city,
    MIN(signup_date) AS min_signup_date,
    MAX(credit_limit) - MIN(credit_limit) AS credit_limit_range
FROM customers
WHERE city IS NOT NULL
GROUP BY 1, 2

SELECT
    YEAR(signup_date) AS signup_year,
    country,
    MIN(age) AS min_age,
    MAX(credit_limit) - MIN(credit_limit) AS credit_limit_range
FROM customers
WHERE country IN ('TH', 'JP')
GROUP BY YEAR(signup_date), country;

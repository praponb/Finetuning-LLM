SELECT
    YEAR(signup_date) AS signup_year,
    country,
    MIN(signup_date) AS min_signup_date,
    MIN(signup_date) AS first_signup_date,
    COUNT(CASE WHEN city = 'Chiang Mai' THEN 1 END) AS chiang_mai_count
FROM customers
GROUP BY YEAR(signup_date), country
ORDER BY country

SELECT
    YEAR(signup_date) AS signup_year,
    MAX(signup_date) AS last_signup_date,
    SUM(CASE WHEN segment = 'Corporate' THEN credit_limit ELSE 0 END) AS corporate_credit_limit,
    COUNT(CASE WHEN segment = 'Corporate' THEN 1 END) AS corporate_count
FROM customers
WHERE (city = 'Bangkok' AND city IN ('Tokyo', 'Chiang Mai', 'Singapore')) OR email LIKE '%@gmail.com'
GROUP BY YEAR(signup_date)
HAVING COUNT(CASE WHEN segment = 'Corporate' THEN 1 END) < 3
ORDER BY corporate_count ASC

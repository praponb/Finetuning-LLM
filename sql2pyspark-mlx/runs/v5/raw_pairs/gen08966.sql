SELECT
    city,
    SUM(CASE WHEN city = 'Singapore' THEN 1 ELSE 0 END) AS singapore_count,
    COUNT(credit_limit) AS credit_limit_count,
    MAX(credit_limit) AS max_credit_limit
FROM customers
GROUP BY city
ORDER BY credit_limit_count DESC, city
LIMIT 5;

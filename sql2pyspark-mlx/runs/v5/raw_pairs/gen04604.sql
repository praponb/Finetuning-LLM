SELECT
    is_active,
    country,
    COUNT(CASE WHEN segment = 'Consumer' THEN 1 END) consumer_count,
    SUM(credit_limit) total_credit_limit
FROM customers
GROUP BY is_active, country
ORDER BY consumer_count DESC

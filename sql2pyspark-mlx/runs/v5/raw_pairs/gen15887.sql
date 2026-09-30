SELECT
    city,
    SUM(CASE WHEN segment = 'Consumer' THEN 1 ELSE 0 END) AS consumer_count,
    SUM(CASE WHEN country = 'SG' THEN 1 ELSE 0 END) AS sg_count,
    COUNT(CASE WHEN city = 'Singapore' THEN 1 END) AS singapore_count
FROM customers
WHERE age = 55 OR city = 'Berlin'
GROUP BY city
ORDER BY sg_count DESC

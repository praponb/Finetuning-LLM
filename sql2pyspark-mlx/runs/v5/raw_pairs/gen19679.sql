SELECT
    city,
    segment,
    MAX(age) - MIN(age) AS age_range,
    ROUND((100 * SUM(CASE WHEN segment = 'Consumer' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_consumer
FROM customers
WHERE is_active = TRUE
GROUP BY city, segment;

SELECT country, stddev_rating, pct_chiang_mai
FROM (
    SELECT country, ROUND(STDDEV(rating), 2) stddev_rating, ROUND((100 * SUM(CASE WHEN city = 'Chiang Mai' THEN 1 ELSE 0 END)) / COUNT(*), 1) pct_chiang_mai
    FROM suppliers
    WHERE country IN ('US', 'TH')
    GROUP BY country
) t
WHERE stddev_rating >= 61
ORDER BY pct_chiang_mai, country
LIMIT 5

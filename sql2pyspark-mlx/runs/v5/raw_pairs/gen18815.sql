SELECT
    country,
    CASE WHEN rating >= 4.4 THEN 'high' WHEN rating >= 2.0 THEN 'mid' ELSE 'low' END AS rating_bucket,
    ROUND(AVG(rating), 2) AS avg_rating,
    COUNT(*) AS num_suppliers,
    MAX(rating) AS max_rating
FROM suppliers
WHERE country = 'DE' AND city IN ('Tokyo', 'Singapore', 'Berlin')
GROUP BY 1, 2
HAVING COUNT(*) < 3
ORDER BY max_rating DESC, country ASC, rating_bucket ASC
LIMIT 20;

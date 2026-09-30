SELECT
    city,
    country,
    CASE WHEN rating >= 4.5 THEN 'high' WHEN rating >= 1.7 THEN 'mid' ELSE 'low' END AS rating_bucket,
    SUM(CASE WHEN city = 'Bangkok' THEN 1 ELSE 0 END) AS bangkok_count,
    AVG(rating) AS avg_rating
FROM suppliers
WHERE city <> 'Bangkok'
GROUP BY city, country, CASE WHEN rating >= 4.5 THEN 'high' WHEN rating >= 1.7 THEN 'mid' ELSE 'low' END
ORDER BY 4 DESC, city, country, rating_bucket
LIMIT 1;

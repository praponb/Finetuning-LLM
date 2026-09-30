SELECT
    country,
    CASE WHEN rating >= 3.8 THEN 'high' WHEN rating >= 1.7 THEN 'mid' ELSE 'low' END AS rating_bucket,
    SUM(CASE WHEN country = 'DE' THEN rating ELSE 0 END) AS de_rating,
    AVG(rating) AS avg_rating,
    ROUND((100 * SUM(CASE WHEN city = 'London' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_london
FROM suppliers
WHERE country = 'SG'
GROUP BY 1, 2
HAVING pct_london >= 43

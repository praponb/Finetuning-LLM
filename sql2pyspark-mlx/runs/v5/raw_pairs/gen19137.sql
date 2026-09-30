SELECT CASE WHEN rating >= 3.8 THEN 'high' WHEN rating >= 2.1 THEN 'mid' ELSE 'low' END AS rating_bucket, SUM(rating) AS total_rating, ROUND((100 * SUM(CASE WHEN country = 'DE' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_de
FROM suppliers
GROUP BY 1
HAVING ROUND((100 * SUM(CASE WHEN country = 'DE' THEN 1 ELSE 0 END)) / COUNT(*), 1) < 26
ORDER BY rating_bucket;

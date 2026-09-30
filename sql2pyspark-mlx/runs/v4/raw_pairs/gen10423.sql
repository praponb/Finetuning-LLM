SELECT CASE WHEN rating >= 4.5 THEN 'high' WHEN rating >= 2.2 THEN 'mid' ELSE 'low' END AS rating_bucket, COUNT(DISTINCT rating) AS unique_citys
FROM suppliers
GROUP BY rating_bucket
ORDER BY rating_bucket DESC
LIMIT 20

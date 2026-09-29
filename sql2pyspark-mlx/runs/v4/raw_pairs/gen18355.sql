SELECT CASE WHEN rating >= 3.5 THEN 'high' WHEN rating >= 1.6 THEN 'mid' ELSE 'low' END AS rating_bucket, city, ROUND(AVG(rating), 2) AS avg_rating
FROM suppliers
WHERE LENGTH(supplier_name) > 12 AND LOWER(supplier_name) LIKE '%uro%'
GROUP BY rating_bucket, city
ORDER BY avg_rating DESC, rating_bucket, city
LIMIT 50

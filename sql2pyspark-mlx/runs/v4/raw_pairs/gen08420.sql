SELECT CASE WHEN rating >= 3.9 THEN 'high' WHEN rating >= 1.6 THEN 'mid' ELSE 'low' END rating_bucket, city, COUNT(rating) rating_count
FROM suppliers
GROUP BY 1, 2
HAVING COUNT(rating) < 4
ORDER BY city DESC

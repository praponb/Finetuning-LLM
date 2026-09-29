SELECT CASE WHEN rating >= 4.1 THEN 'high' WHEN rating >= 2.2 THEN 'mid' ELSE 'low' END rating_bucket, SUM(rating) total_rating, COUNT(rating) rating_count
FROM suppliers
GROUP BY CASE WHEN rating >= 4.1 THEN 'high' WHEN rating >= 2.2 THEN 'mid' ELSE 'low' END
HAVING rating_count >= 4
ORDER BY total_rating DESC, 3

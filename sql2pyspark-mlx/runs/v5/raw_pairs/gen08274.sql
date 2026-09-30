SELECT *
FROM (
    SELECT country, CASE WHEN rating >= 4.1 THEN 'high' WHEN rating >= 2.3 THEN 'mid' ELSE 'low' END rating_bucket, SUM(rating) total_rating
    FROM suppliers
    GROUP BY country, CASE WHEN rating >= 4.1 THEN 'high' WHEN rating >= 2.3 THEN 'mid' ELSE 'low' END
) t
WHERE total_rating > 13
ORDER BY total_rating DESC

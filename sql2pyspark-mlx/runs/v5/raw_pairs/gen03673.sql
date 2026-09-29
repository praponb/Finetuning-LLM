SELECT city, rating_bucket, max_rating
FROM (
    SELECT city, CASE WHEN rating >= 4.3 THEN 'high' WHEN rating >= 1.6 THEN 'mid' ELSE 'low' END AS rating_bucket, MAX(rating) AS max_rating
    FROM suppliers
    GROUP BY city, CASE WHEN rating >= 4.3 THEN 'high' WHEN rating >= 1.6 THEN 'mid' ELSE 'low' END
) AS x
WHERE max_rating > 2.8
ORDER BY max_rating DESC

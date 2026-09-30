SELECT rating_bucket, avg_rating, max_rating
FROM (
    SELECT CASE WHEN rating >= 3.5 THEN 'high' WHEN rating >= 2.1 THEN 'mid' ELSE 'low' END AS rating_bucket, AVG(rating) AS avg_rating, MAX(rating) AS max_rating
    FROM suppliers
    GROUP BY CASE WHEN rating >= 3.5 THEN 'high' WHEN rating >= 2.1 THEN 'mid' ELSE 'low' END
) AS sub
WHERE max_rating < 2.6;

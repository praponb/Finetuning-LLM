SELECT CASE WHEN rating >= 4.0 THEN 'high' WHEN rating >= 2.3 THEN 'mid' ELSE 'low' END AS rating_bucket, SUM(rating) AS total_rating, DENSE_RANK() OVER (ORDER BY SUM(rating) DESC) AS rating_rank
FROM suppliers
GROUP BY CASE WHEN rating >= 4.0 THEN 'high' WHEN rating >= 2.3 THEN 'mid' ELSE 'low' END

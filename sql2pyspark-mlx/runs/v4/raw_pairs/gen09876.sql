SELECT CASE WHEN rating >= 3.5 THEN 'high' WHEN rating >= 2.1 THEN 'mid' ELSE 'low' END AS rating_bucket, SUM(rating) AS total_rating, DENSE_RANK() OVER (ORDER BY SUM(rating) DESC) AS rating_rank
FROM suppliers
WHERE supplier_name LIKE '%Suppl%'
GROUP BY CASE WHEN rating >= 3.5 THEN 'high' WHEN rating >= 2.1 THEN 'mid' ELSE 'low' END
ORDER BY total_rating ASC, rating_bucket ASC
LIMIT 5;

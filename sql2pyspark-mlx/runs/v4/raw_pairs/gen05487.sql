SELECT
    CASE WHEN rating >= 4.4 THEN 'HIGH' ELSE 'LOW' END AS rating_tier,
    city,
    ROUND(AVG(rating), 2) AS avg_rating,
    COUNT(*) AS row_count,
    DENSE_RANK() OVER (PARTITION BY CASE WHEN rating >= 4.4 THEN 'HIGH' ELSE 'LOW' END ORDER BY COUNT(*) DESC) AS row_count_dense_rank
FROM suppliers
GROUP BY 1, 2
HAVING COUNT(*) >= 1
ORDER BY city

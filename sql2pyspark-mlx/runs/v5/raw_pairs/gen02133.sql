SELECT store_id, ROUND(AVG(revenue), 2) AS avg_revenue, COUNT(*) AS num_store_sales
FROM store_sales
WHERE units BETWEEN 16 AND 23
GROUP BY store_id
HAVING avg_revenue < 53

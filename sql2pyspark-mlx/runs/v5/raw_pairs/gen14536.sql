SELECT CASE WHEN quantity >= 7 THEN 'high' WHEN quantity >= 4 THEN 'mid' ELSE 'low' END AS quantity_bucket, SUM(quantity) AS total_quantity, RANK() OVER (ORDER BY SUM(quantity) DESC) AS quantity_rank
FROM order_items
GROUP BY CASE WHEN quantity >= 7 THEN 'high' WHEN quantity >= 4 THEN 'mid' ELSE 'low' END
ORDER BY total_quantity DESC;

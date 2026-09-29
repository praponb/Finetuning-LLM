SELECT CASE WHEN unit_price >= 330 THEN 'high' WHEN unit_price >= 150 THEN 'mid' ELSE 'low' END AS unit_price_bucket, SUM(quantity) AS total_quantity, AVG(unit_price) AS avg_unit_price
FROM order_items
GROUP BY CASE WHEN unit_price >= 330 THEN 'high' WHEN unit_price >= 150 THEN 'mid' ELSE 'low' END
ORDER BY unit_price_bucket, total_quantity DESC;

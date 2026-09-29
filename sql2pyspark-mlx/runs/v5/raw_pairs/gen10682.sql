SELECT store_id, ROUND(AVG(quantity), 2) AS avg_quantity FROM inventory GROUP BY store_id HAVING MIN(quantity) < 180 ORDER BY store_id DESC, avg_quantity LIMIT 3;

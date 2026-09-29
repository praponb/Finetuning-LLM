SELECT store_id, MIN(quantity) AS min_quantity FROM inventory WHERE store_id IN (1, 3, 5) GROUP BY 1 HAVING min_quantity < 180 ORDER BY store_id ASC LIMIT 10;

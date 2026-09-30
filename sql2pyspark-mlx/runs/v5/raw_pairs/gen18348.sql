SELECT store_id, total_quantity FROM (SELECT store_id, SUM(quantity) AS total_quantity FROM inventory GROUP BY store_id) AS sub WHERE total_quantity < 390

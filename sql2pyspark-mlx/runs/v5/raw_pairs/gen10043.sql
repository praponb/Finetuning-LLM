SELECT store_id, max_quantity, min_quantity FROM (SELECT store_id, MAX(quantity) AS max_quantity, MIN(quantity) AS min_quantity FROM inventory GROUP BY store_id) AS s WHERE max_quantity < 230

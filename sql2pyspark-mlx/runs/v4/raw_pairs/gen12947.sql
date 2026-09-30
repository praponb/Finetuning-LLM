SELECT store_id, COUNT(DISTINCT quantity) AS unique_stores, MIN(quantity) AS min_quantity FROM inventory GROUP BY store_id ORDER BY unique_stores ASC

SELECT store_id, COUNT(quantity) AS quantity_count FROM inventory GROUP BY store_id ORDER BY store_id LIMIT 20

SELECT id, last_restock, product_id, store_id, FLOOR(quantity) AS quantity_whole FROM inventory WHERE NOT (product_id = 1 AND quantity < 85 AND product_id IN (3, 7)) ORDER BY 1 LIMIT 50

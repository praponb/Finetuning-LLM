SELECT id, last_restock, product_id, COALESCE(quantity, 0) AS quantity_filled FROM inventory WHERE MONTH(last_restock) IN (1, 11, 12)

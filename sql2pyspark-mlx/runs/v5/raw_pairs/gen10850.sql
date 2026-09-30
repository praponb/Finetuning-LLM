SELECT last_restock, quantity, COALESCE(quantity, 0) AS quantity_filled FROM inventory WHERE product_id = 6

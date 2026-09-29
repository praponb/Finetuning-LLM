SELECT id, quantity, store_id, COALESCE(quantity, 0) AS quantity_filled FROM inventory WHERE product_id IN (4, 6, 8, 9);

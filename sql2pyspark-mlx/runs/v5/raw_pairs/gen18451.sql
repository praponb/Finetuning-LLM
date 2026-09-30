SELECT id, store_id, last_restock, quantity FROM inventory WHERE quantity = 138 AND product_id IN (3, 5, 7, 8) AND store_id = 2;

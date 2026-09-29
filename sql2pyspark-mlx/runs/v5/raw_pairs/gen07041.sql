SELECT id, last_restock, quantity, store_id FROM inventory WHERE product_id IN (4, 5, 6, 7) AND store_id IN (1, 3, 6) AND product_id IN (3, 7, 9, 10) ORDER BY store_id;

SELECT id, store_id, last_restock, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY last_restock, id) AS rn FROM inventory WHERE product_id IN (1, 5, 9);

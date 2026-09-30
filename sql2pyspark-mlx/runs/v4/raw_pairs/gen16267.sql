SELECT id, product_id, store_id FROM inventory WHERE product_id IN (4, 10) EXCEPT SELECT id, product_id, store_id FROM inventory WHERE last_restock > '2023-08-21' ORDER BY store_id;

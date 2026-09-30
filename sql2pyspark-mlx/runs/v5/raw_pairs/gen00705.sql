SELECT id, store_id FROM orders EXCEPT ALL SELECT id, store_id FROM orders WHERE store_id IN (7, 8, 21, 31);

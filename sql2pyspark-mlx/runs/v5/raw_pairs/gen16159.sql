SELECT id, last_restock FROM inventory WHERE quantity < 69 UNION ALL SELECT id, last_restock FROM inventory WHERE store_id IN (4, 5, 6);

SELECT quantity FROM inventory WHERE store_id IN (4, 6) UNION SELECT quantity FROM inventory WHERE last_restock > '2023-12-03';

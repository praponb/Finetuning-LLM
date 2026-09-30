SELECT id, quantity FROM inventory WHERE store_id IN (1, 3, 5, 6) INTERSECT SELECT id, quantity FROM inventory WHERE MONTH(last_restock) IN (2, 6, 10) ORDER BY id DESC;

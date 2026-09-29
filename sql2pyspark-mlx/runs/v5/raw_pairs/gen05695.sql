SELECT store_id FROM inventory WHERE store_id IN (1, 2, 3, 6) UNION ALL SELECT store_id FROM inventory WHERE last_restock > '2023-12-01' ORDER BY store_id;

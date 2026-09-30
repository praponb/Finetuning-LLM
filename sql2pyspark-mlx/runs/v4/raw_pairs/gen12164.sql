SELECT store_id FROM inventory WHERE store_id IN (3, 4, 5) UNION SELECT store_id FROM inventory WHERE quantity = 143;

SELECT store_id FROM inventory WHERE quantity = 139 EXCEPT SELECT store_id FROM inventory WHERE store_id IN (3, 6)

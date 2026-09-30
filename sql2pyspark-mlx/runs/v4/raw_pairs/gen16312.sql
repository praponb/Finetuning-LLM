SELECT id, product_id FROM inventory INTERSECT SELECT id, product_id FROM inventory WHERE store_id IN (1, 3, 5, 6)

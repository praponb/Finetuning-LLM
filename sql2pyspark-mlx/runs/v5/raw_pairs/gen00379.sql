SELECT id, product_id FROM inventory WHERE store_id IN (1, 2, 3) AND (product_id IN (1, 4, 6, 8) OR quantity <= 80) ORDER BY id

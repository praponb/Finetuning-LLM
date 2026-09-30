SELECT product_id FROM inventory WHERE store_id IN (1, 2, 5) UNION SELECT product_id FROM inventory WHERE store_id = 6 ORDER BY product_id

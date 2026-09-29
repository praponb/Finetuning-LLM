SELECT product_id FROM inventory WHERE product_id IN (1, 7) UNION SELECT product_id FROM inventory WHERE store_id IN (2, 4, 5, 6) ORDER BY product_id

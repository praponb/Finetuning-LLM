SELECT product_id FROM inventory WHERE store_id IN (1, 3, 6) UNION SELECT product_id FROM inventory WHERE store_id IN (1, 4, 5, 6) ORDER BY product_id DESC

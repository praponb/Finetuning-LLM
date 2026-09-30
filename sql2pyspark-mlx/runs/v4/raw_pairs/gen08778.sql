SELECT quantity, product_id FROM inventory WHERE product_id IN (2, 7, 10) UNION SELECT quantity, product_id FROM inventory WHERE store_id IN (1, 2, 3, 5) ORDER BY product_id DESC;

SELECT id, quantity, product_id FROM inventory WHERE quantity <= (SELECT MIN(quantity) FROM inventory WHERE store_id IN (1, 3)) ORDER BY quantity, id DESC;

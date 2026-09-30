SELECT id, product_id, quantity, ROUND(quantity, 2) AS quantity_rounded FROM inventory WHERE quantity <> 171 OR product_id IN (1, 5, 6, 10) ORDER BY product_id, quantity DESC

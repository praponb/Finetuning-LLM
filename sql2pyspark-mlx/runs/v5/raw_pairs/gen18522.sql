SELECT id, unit_price, product_id, quantity FROM order_items WHERE order_id IN (8, 9, 18, 19) ORDER BY id LIMIT 5;

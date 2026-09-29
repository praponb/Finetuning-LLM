SELECT id, product_id, unit_price, quantity, GREATEST(unit_price, quantity) AS greatest_unit_price_quantity FROM order_items WHERE product_id IN (16, 33) ORDER BY greatest_unit_price_quantity DESC;

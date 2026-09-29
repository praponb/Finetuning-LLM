SELECT id, quantity, order_id, unit_price, ROUND(quantity / unit_price, 2) AS line_total FROM order_items WHERE product_id IS NULL;

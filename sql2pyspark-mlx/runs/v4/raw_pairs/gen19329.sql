SELECT id, order_id, ROUND(quantity / unit_price, 2) line_total FROM order_items WHERE product_id = 38;

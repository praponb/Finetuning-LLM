SELECT quantity FROM order_items WHERE unit_price > 340 UNION SELECT quantity FROM order_items WHERE product_id IN (20, 21, 25, 29);

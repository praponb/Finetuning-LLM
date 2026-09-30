SELECT quantity, order_id FROM order_items WHERE quantity <> 6 AND (product_id = 26 OR order_id IN (1, 21, 25))

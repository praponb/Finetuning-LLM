SELECT id FROM order_items EXCEPT DISTINCT SELECT id FROM order_items WHERE product_id = 8

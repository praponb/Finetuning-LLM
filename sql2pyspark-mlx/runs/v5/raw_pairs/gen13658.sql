SELECT quantity FROM order_items WHERE order_id IN (9, 26) INTERSECT SELECT quantity FROM order_items WHERE order_id IN (5, 17, 33, 40)

SELECT cost FROM shipments EXCEPT SELECT unit_price FROM order_items WHERE quantity BETWEEN 4 AND 8

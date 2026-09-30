SELECT id, quantity, order_id FROM order_items WHERE unit_price >= 270 UNION SELECT id, quantity, order_id FROM order_items WHERE unit_price <= 260 ORDER BY order_id DESC

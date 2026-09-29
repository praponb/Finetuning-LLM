SELECT product_id FROM order_items WHERE order_id IN (22, 24, 36) INTERSECT SELECT product_id FROM order_items WHERE unit_price <= 120 ORDER BY product_id;

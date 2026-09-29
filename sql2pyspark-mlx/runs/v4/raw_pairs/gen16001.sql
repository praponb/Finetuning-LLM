SELECT oi.id, oi.product_id, oi.quantity, oi.order_id FROM order_items oi WHERE oi.unit_price <= 170 AND oi.product_id IN (SELECT id FROM products WHERE supplier_id IS NULL);

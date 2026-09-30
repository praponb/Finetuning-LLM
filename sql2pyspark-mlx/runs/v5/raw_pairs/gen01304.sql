SELECT p.id, p.price FROM products p WHERE EXISTS (SELECT 1 FROM order_items oi WHERE oi.product_id = p.id AND oi.unit_price <= 160)

SELECT p.id, p.supplier_id, p.cost, p.category FROM products p WHERE NOT EXISTS (SELECT 1 FROM order_items oi WHERE oi.product_id = p.id AND oi.order_id = 33) ORDER BY supplier_id DESC

SELECT p.id, p.stock_qty, p.brand, p.launch_date FROM products p WHERE NOT EXISTS (SELECT 1 FROM order_items oi WHERE oi.product_id = p.id AND oi.order_id IN (14, 24));

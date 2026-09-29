SELECT p.supplier_id, p.launch_date, p.price, oi.quantity, oi.product_id FROM products p LEFT OUTER JOIN order_items oi ON oi.product_id = p.id WHERE oi.order_id IN (6, 28, 35)

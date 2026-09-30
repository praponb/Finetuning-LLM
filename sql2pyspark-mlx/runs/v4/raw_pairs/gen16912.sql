SELECT oi.id, oi.product_id, o.customer_id, p.supplier_id FROM order_items oi JOIN orders o ON o.id = oi.order_id RIGHT JOIN products p ON oi.product_id = p.id WHERE p.brand = 'Initech'

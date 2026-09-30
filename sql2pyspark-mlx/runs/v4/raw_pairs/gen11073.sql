SELECT p.product_name, oi.order_id, oi.unit_price FROM products p JOIN order_items oi ON oi.product_id = p.id WHERE p.cost < 640

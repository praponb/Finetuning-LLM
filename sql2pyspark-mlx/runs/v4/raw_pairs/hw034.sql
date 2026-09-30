SELECT p.product_name, oi.quantity FROM products p RIGHT JOIN order_items oi ON p.id = oi.product_id;

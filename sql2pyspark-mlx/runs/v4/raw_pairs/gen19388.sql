SELECT p.cost, p.price, p.category, oi.product_id FROM products p LEFT OUTER JOIN order_items oi ON p.id = oi.product_id;

SELECT p.id, p.category, COUNT(oi.id) AS order_item_count FROM products p LEFT JOIN order_items oi ON oi.product_id = p.id GROUP BY p.id, p.category ORDER BY p.id DESC, category ASC LIMIT 10;

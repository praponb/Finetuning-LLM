SELECT p.id, p.brand, COUNT(oi.id) AS order_item_count FROM products p LEFT JOIN order_items oi ON oi.product_id = p.id GROUP BY p.id, p.brand ORDER BY p.id DESC, p.brand

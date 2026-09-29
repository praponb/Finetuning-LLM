SELECT p.cost, oi.order_id
FROM products p
JOIN order_items oi ON oi.product_id = p.id

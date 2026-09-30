SELECT o.discount, oi.unit_price, oi.order_id
FROM orders o
LEFT JOIN order_items oi ON oi.order_id = o.id

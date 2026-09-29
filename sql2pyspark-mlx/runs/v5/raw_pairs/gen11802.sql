SELECT p.order_id, p.method, o.status
FROM payments p
LEFT JOIN orders o ON o.id = p.order_id
ORDER BY p.order_id DESC

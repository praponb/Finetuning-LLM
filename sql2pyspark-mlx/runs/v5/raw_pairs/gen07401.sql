SELECT p.status, p.paid_at, o.discount
FROM payments p
INNER JOIN orders o ON p.order_id = o.id
ORDER BY status DESC

SELECT p.id, p.amount, p.status
FROM payments p
WHERE p.method = 'card' AND p.order_id IN (SELECT id FROM orders WHERE status <> 'PENDING');

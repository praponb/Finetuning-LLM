SELECT p.id, p.amount, p.status
FROM payments p
WHERE p.paid_at >= '2023-11-01' AND p.order_id IN (SELECT id FROM orders WHERE status = 'CANCELLED');

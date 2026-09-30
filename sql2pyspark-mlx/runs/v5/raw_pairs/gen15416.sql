SELECT p.id, p.paid_at, p.method FROM payments p WHERE p.order_id IN (SELECT id FROM orders WHERE status IN ('RETURNED', 'PENDING', 'CANCELLED'))

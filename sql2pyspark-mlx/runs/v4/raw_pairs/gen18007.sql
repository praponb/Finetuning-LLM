SELECT p.id, p.paid_at, p.method, p.status FROM payments p WHERE p.order_id IN (6, 13, 14, 28) AND p.order_id NOT IN (SELECT id FROM orders WHERE status IN ('PENDING', 'SHIPPED'))

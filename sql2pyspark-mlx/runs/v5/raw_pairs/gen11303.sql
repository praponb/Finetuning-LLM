SELECT p.id, p.method, p.paid_at, p.status FROM payments p WHERE p.order_id NOT IN (SELECT id FROM orders WHERE channel = 'web');

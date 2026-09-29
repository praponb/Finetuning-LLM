SELECT p.id, p.paid_at, p.method, p.order_id FROM payments p WHERE p.order_id IN (SELECT id FROM orders WHERE discount BETWEEN 0.21 AND 0.3);

SELECT p.id, p.amount, p.status, p.paid_at FROM payments p WHERE p.order_id IN (SELECT id FROM orders WHERE discount BETWEEN 0.15 AND 0.29)

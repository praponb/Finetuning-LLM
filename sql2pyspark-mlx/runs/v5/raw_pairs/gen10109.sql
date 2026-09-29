SELECT p.id, p.amount FROM payments p WHERE p.order_id IN (SELECT id FROM orders WHERE channel IN ('web', 'partner'))

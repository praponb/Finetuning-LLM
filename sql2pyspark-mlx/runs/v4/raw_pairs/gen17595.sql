SELECT p.id, p.order_id, p.paid_at FROM payments p WHERE p.order_id IN (SELECT id FROM orders WHERE amount BETWEEN 1010 AND 1220) ORDER BY order_id, paid_at

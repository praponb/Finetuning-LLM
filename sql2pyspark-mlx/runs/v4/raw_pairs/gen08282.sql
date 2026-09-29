SELECT p.id, p.paid_at, p.amount FROM payments p WHERE p.order_id IN (SELECT id FROM orders WHERE amount <= 1330) ORDER BY amount DESC, p.paid_at DESC, id LIMIT 3;

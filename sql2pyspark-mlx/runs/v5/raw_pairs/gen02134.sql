SELECT p.id, p.paid_at, p.amount FROM payments p WHERE p.order_id NOT IN (SELECT id FROM orders WHERE status = 'COMPLETED') ORDER BY paid_at ASC, amount DESC

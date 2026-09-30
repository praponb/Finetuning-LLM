SELECT p.id, p.status, p.amount, p.order_id FROM payments p WHERE p.amount = (SELECT MIN(p2.amount) FROM payments p2 WHERE p2.status = p.status)

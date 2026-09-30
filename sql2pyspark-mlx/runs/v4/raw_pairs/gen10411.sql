SELECT p.id, p.status, p.amount, p.paid_at FROM payments p WHERE p.amount < (SELECT AVG(p2.amount) FROM payments p2 WHERE p2.status = p.status)

SELECT o.id, o.status, o.amount FROM orders o WHERE o.amount < (SELECT AVG(o2.amount) FROM orders o2 WHERE o2.status = o.status)

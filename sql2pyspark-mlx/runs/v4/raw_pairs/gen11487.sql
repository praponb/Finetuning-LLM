SELECT p.order_id, p.status, p.method, o.order_date FROM payments p RIGHT JOIN orders o ON p.order_id = o.id WHERE p.amount BETWEEN 870 AND 950 OR p.paid_at >= '2023-04-01' ORDER BY method DESC;

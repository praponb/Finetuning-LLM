SELECT p.id, p.paid_at, p.order_id, o.status, o.order_date FROM payments p RIGHT JOIN orders o ON p.order_id = o.id ORDER BY order_date

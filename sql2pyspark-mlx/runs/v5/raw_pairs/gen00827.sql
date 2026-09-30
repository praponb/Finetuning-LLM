SELECT p.id, p.paid_at, o.order_date FROM payments p JOIN orders o ON p.order_id = o.id;

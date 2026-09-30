SELECT p.paid_at, p.status, p.amount, o.order_date, o.amount order_amount FROM payments p LEFT JOIN orders o ON p.order_id = o.id

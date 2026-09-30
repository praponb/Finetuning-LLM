SELECT p.id, p.order_id, p.amount, o.amount AS order_amount FROM payments p LEFT JOIN orders o ON p.order_id = o.id WHERE o.order_date < '2024-07-01';

SELECT p.id, p.amount, o.discount
FROM payments p
INNER JOIN orders o ON p.order_id = o.id;

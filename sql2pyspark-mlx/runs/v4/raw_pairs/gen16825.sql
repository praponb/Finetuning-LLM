SELECT p.id, p.amount, o.customer_id FROM payments p INNER JOIN orders o ON o.id = p.order_id

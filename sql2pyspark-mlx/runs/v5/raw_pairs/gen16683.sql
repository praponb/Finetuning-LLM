SELECT p.id, p.amount, o.discount, o.status FROM payments p INNER JOIN orders o ON o.id = p.order_id ORDER BY o.status DESC;

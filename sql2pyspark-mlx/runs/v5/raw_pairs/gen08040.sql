SELECT p.id, p.method, p.status, p.amount, o.discount FROM payments p RIGHT OUTER JOIN orders o ON o.id = p.order_id;

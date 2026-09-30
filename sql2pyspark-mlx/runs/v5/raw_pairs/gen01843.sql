SELECT p.status, o.status AS order_status, o.amount FROM payments p INNER JOIN orders o ON o.id = p.order_id ORDER BY p.status DESC;

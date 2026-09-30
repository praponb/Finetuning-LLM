SELECT p.id, p.status, o.status AS order_status
FROM payments p
INNER JOIN orders o ON o.id = p.order_id;

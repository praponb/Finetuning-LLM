SELECT p.id, p.method, o.discount FROM payments p JOIN orders o ON p.order_id = o.id ORDER BY o.discount DESC, id DESC;

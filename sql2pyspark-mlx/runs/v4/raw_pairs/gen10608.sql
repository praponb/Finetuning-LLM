SELECT p.id, p.method, p.order_id, p.status, o.store_id FROM payments p LEFT JOIN orders o ON o.id = p.order_id ORDER BY o.store_id, id LIMIT 50;

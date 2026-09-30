SELECT p.method, p.order_id, p.paid_at, o.store_id, o.status, o.customer_id FROM payments p INNER JOIN orders o ON p.order_id = o.id ORDER BY o.status, store_id DESC;

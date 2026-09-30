SELECT p.paid_at, o.order_date, o.store_id FROM payments p JOIN orders o ON p.order_id = o.id ORDER BY paid_at DESC;

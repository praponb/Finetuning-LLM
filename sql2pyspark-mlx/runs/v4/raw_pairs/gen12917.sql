SELECT o.id, o.order_date FROM orders o WHERE o.customer_id = 28 AND NOT EXISTS (SELECT 1 FROM payments p WHERE p.order_id = o.id AND p.paid_at >= '2025-03-01')

SELECT o.id, o.amount FROM orders o WHERE NOT EXISTS (SELECT 1 FROM payments p WHERE p.order_id = o.id AND p.paid_at >= '2023-05-20')

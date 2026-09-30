SELECT o.id, o.store_id, o.customer_id, o.status FROM orders o WHERE NOT EXISTS (SELECT 1 FROM payments p WHERE p.order_id = o.id)

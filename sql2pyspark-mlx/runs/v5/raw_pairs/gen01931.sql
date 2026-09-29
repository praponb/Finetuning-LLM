SELECT o.id, o.store_id, o.amount, o.order_date FROM orders o WHERE o.id IN (SELECT order_id FROM shipments WHERE status NOT IN ('LOST', 'IN_TRANSIT'))

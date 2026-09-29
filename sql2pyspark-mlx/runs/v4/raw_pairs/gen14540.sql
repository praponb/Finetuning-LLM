SELECT o.id, o.channel FROM orders o WHERE NOT EXISTS (SELECT 1 FROM shipments s WHERE s.order_id = o.id)

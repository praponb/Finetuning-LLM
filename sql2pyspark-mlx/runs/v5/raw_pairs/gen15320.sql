SELECT s.* FROM orders s WHERE NOT EXISTS (SELECT 1 FROM shipments c WHERE c.order_id = s.id AND c.order_id IS NOT NULL)

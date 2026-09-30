SELECT id FROM shipments INTERSECT SELECT id FROM shipments WHERE order_id IS NOT NULL

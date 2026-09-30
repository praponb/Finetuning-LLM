SELECT status FROM shipments INTERSECT ALL SELECT status FROM shipments WHERE status IN ('DELIVERED', 'IN_TRANSIT')

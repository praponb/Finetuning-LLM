SELECT id, carrier FROM shipments EXCEPT ALL SELECT id, carrier FROM shipments WHERE status = 'IN_TRANSIT'

SELECT status, shipped_date FROM shipments WHERE carrier IS NULL OR status = 'DELIVERED' ORDER BY status DESC

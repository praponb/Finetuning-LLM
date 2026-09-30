SELECT id, status, COUNT(*) OVER (PARTITION BY status) AS status_count FROM shipments WHERE carrier = 'DHL';

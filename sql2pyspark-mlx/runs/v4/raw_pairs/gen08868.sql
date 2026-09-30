SELECT * FROM shipments QUALIFY ROW_NUMBER() OVER (PARTITION BY status, carrier ORDER BY cost DESC, id) = 1

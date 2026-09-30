SELECT status, carrier, shipped_date
FROM shipments
WHERE status IN ('DELIVERED', 'LOST', 'RETURNED') OR cost > 87

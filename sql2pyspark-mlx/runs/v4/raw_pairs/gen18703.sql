SELECT * FROM (SELECT carrier, status, cost FROM shipments WHERE status = 'LOST')
PIVOT (MAX(cost) FOR status IN ('DELIVERED', 'RETURNED', 'LOST'))

SELECT * FROM (SELECT carrier, status, cost FROM shipments)
PIVOT (SUM(cost) FOR status IN ('RETURNED', 'LOST'))
ORDER BY carrier

SELECT * FROM (SELECT carrier, status, cost FROM shipments WHERE cost BETWEEN 33 AND 87)
PIVOT (COUNT(cost) FOR status IN ('LOST', 'IN_TRANSIT', 'DELIVERED'))
ORDER BY carrier;

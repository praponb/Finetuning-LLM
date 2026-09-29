SELECT * FROM (SELECT carrier, status, cost FROM shipments)
PIVOT (AVG(cost) FOR status IN ('RETURNED', 'DELIVERED'))
ORDER BY carrier;

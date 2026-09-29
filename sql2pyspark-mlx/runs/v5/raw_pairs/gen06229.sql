SELECT * FROM (SELECT carrier, status, cost FROM shipments) PIVOT (AVG(cost) FOR status IN ('DELIVERED', 'IN_TRANSIT')) ORDER BY carrier;

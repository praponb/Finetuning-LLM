SELECT * FROM (SELECT carrier, status, cost FROM shipments) PIVOT (SUM(cost) FOR status IN ('DELIVERED', 'RETURNED', 'IN_TRANSIT')) ORDER BY carrier;

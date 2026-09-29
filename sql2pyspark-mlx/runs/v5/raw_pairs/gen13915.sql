SELECT * FROM (SELECT carrier, status, cost FROM shipments WHERE shipped_date <= '2024-06-28') PIVOT (AVG(cost) FOR status IN ('LOST', 'IN_TRANSIT', 'DELIVERED', 'RETURNED')) ORDER BY carrier;

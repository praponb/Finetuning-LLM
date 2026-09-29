SELECT * FROM (SELECT status, carrier, cost FROM shipments) PIVOT (COUNT(cost) FOR carrier IN ('UPS', 'FedEx')) ORDER BY status

SELECT * FROM (SELECT status, carrier, cost FROM shipments) PIVOT (COUNT(cost) FOR carrier IN ('FedEx', 'Kerry')) ORDER BY status

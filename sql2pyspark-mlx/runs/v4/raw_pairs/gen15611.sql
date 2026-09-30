SELECT * FROM (SELECT status, carrier, cost FROM shipments WHERE delivered_date >= '2024-12-07') PIVOT (SUM(cost) FOR carrier IN ('FedEx', 'DHL', 'UPS')) ORDER BY status;

SELECT * FROM (SELECT status, carrier, cost FROM shipments WHERE delivered_date >= '2025-01-01')
PIVOT (SUM(cost) FOR carrier IN ('Kerry', 'FedEx', 'UPS', 'Flash'))
ORDER BY status;

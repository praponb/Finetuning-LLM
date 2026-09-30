SELECT * FROM (SELECT QUARTER(shipped_date) AS shipped_quarter, status, cost FROM shipments WHERE delivered_date >= '2024-04-01')
PIVOT (AVG(cost) FOR status IN ('RETURNED', 'IN_TRANSIT', 'DELIVERED', 'LOST'));

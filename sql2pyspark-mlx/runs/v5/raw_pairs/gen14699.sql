SELECT * FROM (SELECT carrier, status, cost FROM shipments WHERE cost BETWEEN 80 AND 84) PIVOT (COUNT(cost) FOR status IN ('RETURNED', 'DELIVERED', 'LOST', 'IN_TRANSIT'));

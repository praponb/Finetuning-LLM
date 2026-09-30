SELECT CASE WHEN carrier IN ('UPS', 'FedEx') THEN 'priority' ELSE 'other' END AS carrier_group, ROUND((100 * SUM(CASE WHEN status = 'RETURNED' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_returned, COUNT(*) AS num_shipments
FROM shipments
GROUP BY CASE WHEN carrier IN ('UPS', 'FedEx') THEN 'priority' ELSE 'other' END
HAVING COUNT(DISTINCT cost) < 4

SELECT status, COUNT(*) AS cnt
FROM shipments
WHERE delivered_date >= '2025-01-24' AND order_id IN (13, 23, 36) AND order_id = 36
GROUP BY status
HAVING COUNT(*) < 5;

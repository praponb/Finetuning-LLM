SELECT carrier, ROUND((100 * SUM(CASE WHEN status = 'DELIVERED' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_delivered
FROM shipments
GROUP BY carrier
HAVING ROUND((100 * SUM(CASE WHEN status = 'DELIVERED' THEN 1 ELSE 0 END)) / COUNT(*), 1) < 40
ORDER BY pct_delivered, carrier

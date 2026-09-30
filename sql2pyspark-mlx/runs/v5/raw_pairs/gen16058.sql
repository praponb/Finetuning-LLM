SELECT id, cost, carrier
FROM shipments
WHERE cost <= (SELECT MIN(cost) FROM shipments)
ORDER BY cost DESC;

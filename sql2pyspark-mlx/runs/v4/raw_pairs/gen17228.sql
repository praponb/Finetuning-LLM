SELECT status, ROUND((100 * SUM(CASE WHEN status = 'IN_TRANSIT' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_in_transit, MAX(cost) AS max_cost
FROM shipments
WHERE carrier = 'Kerry'
GROUP BY status
HAVING max_cost < 51
ORDER BY pct_in_transit DESC;

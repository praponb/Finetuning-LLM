SELECT id, status, cost FROM (SELECT *, DENSE_RANK() OVER (PARTITION BY status ORDER BY cost DESC) AS rnk FROM shipments) AS ranked WHERE rnk = 1 ORDER BY status, cost DESC

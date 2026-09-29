SELECT id, status, cost, MIN(cost) OVER (PARTITION BY status) AS status_min_cost FROM shipments

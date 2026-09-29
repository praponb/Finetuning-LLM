SELECT id, delivered_date, cost, status, LEAD(cost, 1) OVER (ORDER BY delivered_date, id) AS next_cost, MIN(cost) OVER (PARTITION BY status) AS status_min_cost FROM shipments;

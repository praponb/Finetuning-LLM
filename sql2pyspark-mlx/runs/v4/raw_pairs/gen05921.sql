SELECT id, shipped_date, cost, LEAD(cost, 2, 0) OVER (ORDER BY shipped_date, id) AS next_cost FROM shipments WHERE carrier IS NOT NULL

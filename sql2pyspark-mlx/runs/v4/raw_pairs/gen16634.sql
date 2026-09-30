SELECT id, shipped_date, cost, SUM(cost) OVER (ORDER BY shipped_date, id) running_cost FROM shipments WHERE status <> 'IN_TRANSIT' ORDER BY running_cost DESC

SELECT id, carrier, status, shipped_date, cost, LAG(cost, 1, 0) OVER (PARTITION BY carrier, status ORDER BY shipped_date, id) AS prev_cost FROM shipments

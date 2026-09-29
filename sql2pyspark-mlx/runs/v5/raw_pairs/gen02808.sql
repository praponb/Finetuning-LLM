SELECT
    id,
    status,
    shipped_date,
    cost,
    SUM(cost) OVER (PARTITION BY status ORDER BY shipped_date, id) running_cost
FROM shipments

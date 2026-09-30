SELECT
    id,
    shipped_date,
    cost,
    status,
    delivered_date,
    SUM(cost) OVER (ORDER BY shipped_date, id) running_cost,
    SUM(cost) OVER (PARTITION BY status ORDER BY delivered_date, id) running_cost_2
FROM shipments

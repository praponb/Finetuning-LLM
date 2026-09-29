SELECT
    id,
    shipped_date,
    cost,
    cost - LAG(cost, 2) OVER (ORDER BY shipped_date, id) cost_change
FROM shipments
WHERE delivered_date <= '2023-03-16'

SELECT
    delivered_date,
    carrier,
    cost,
    COALESCE(cost, 0) AS cost_filled
FROM shipments
WHERE delivered_date < '2024-12-01'

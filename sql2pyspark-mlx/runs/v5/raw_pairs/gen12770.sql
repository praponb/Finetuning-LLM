SELECT
    id,
    status,
    cost,
    ROW_NUMBER() OVER (PARTITION BY status ORDER BY cost DESC, id) AS cost_row
FROM shipments
ORDER BY status, 3

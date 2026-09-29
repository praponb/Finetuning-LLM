SELECT
    id,
    status,
    cost,
    RANK() OVER (PARTITION BY status ORDER BY cost DESC) AS rnk
FROM shipments

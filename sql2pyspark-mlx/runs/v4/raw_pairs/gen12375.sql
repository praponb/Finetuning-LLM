SELECT
    id,
    cost,
    status,
    CASE WHEN status IN ('DELIVERED', 'IN_TRANSIT') THEN 'Core' ELSE 'Other' END AS status_group,
    CAST(cost AS INT) AS cost_int
FROM shipments
WHERE status = 'DELIVERED'
ORDER BY cost DESC;

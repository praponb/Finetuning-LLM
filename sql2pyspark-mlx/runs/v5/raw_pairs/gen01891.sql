SELECT
    id,
    cost,
    LENGTH(status) AS status_length,
    FLOOR(cost) AS cost_whole
FROM shipments
WHERE order_id IS NOT NULL AND carrier IN ('UPS', 'Flash')
ORDER BY cost DESC, id ASC
LIMIT 50

SELECT
    id,
    order_id,
    carrier,
    LOWER(carrier) AS carrier_lower
FROM shipments
WHERE status IN ('LOST', 'DELIVERED', 'IN_TRANSIT') AND (order_id IS NULL OR order_id IS NULL)
ORDER BY carrier

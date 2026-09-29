SELECT
    order_id,
    carrier,
    status,
    CAST(cost AS INT) AS cost_int,
    CASE WHEN carrier IN ('Flash', 'FedEx') THEN 'Priority' ELSE 'Other' END AS carrier_group
FROM shipments
WHERE status <> 'RETURNED'
ORDER BY status

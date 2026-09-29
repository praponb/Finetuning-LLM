SELECT
    id,
    status,
    order_id,
    delivered_date,
    CAST(cost AS INT) AS cost_int,
    LOWER(status) AS status_lower
FROM shipments
WHERE status IN ('IN_TRANSIT', 'RETURNED') AND status != 'RETURNED';

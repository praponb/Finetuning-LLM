WITH ranked AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        customer_id,
        device,
        started_at
    FROM sessions
    QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id, device ORDER BY started_at DESC, id) = 1
)
SELECT
    id,
    device,
    started_at,
    CASE WHEN device RLIKE '^(desktop|tablet)$' THEN 'CORE' WHEN device RLIKE '^(mobile)$' THEN 'GROUP_B' ELSE 'GROUP_A' END AS device_zone
FROM ranked

WITH deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY cost DESC NULLS FIRST, id) AS rn
        FROM shipments
    ) t
    WHERE rn = 1
)
SELECT
    id,
    order_id,
    carrier,
    shipped_date,
    delivered_date,
    cost,
    status,
    rn,
    CASE WHEN status RLIKE '^(RETURNED|LOST)$' THEN 'CORE' WHEN status RLIKE '^(IN_TRANSIT|DELIVERED)$' THEN 'EXTENDED' ELSE 'OTHER_ZONE' END AS status_zone
FROM deduped

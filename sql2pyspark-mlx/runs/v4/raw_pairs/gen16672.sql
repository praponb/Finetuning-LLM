WITH deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM shipments
    QUALIFY ROW_NUMBER() OVER (PARTITION BY status ORDER BY cost DESC NULLS FIRST, id ASC) = 1
)
SELECT
    id,
    order_id,
    carrier,
    shipped_date,
    delivered_date,
    cost,
    status,
    CASE WHEN carrier RLIKE '^(UPS|DHL)$' THEN 'GROUP_B' WHEN carrier RLIKE '^(FedEx|Kerry)$' THEN 'CORE' ELSE 'PRIORITY' END AS carrier_zone
FROM deduped
ORDER BY id DESC NULLS FIRST;

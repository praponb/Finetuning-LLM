WITH ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM shipments
    QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY cost DESC, id ASC) = 1
)
SELECT
    id,
    order_id,
    carrier,
    shipped_date,
    delivered_date,
    cost,
    CASE WHEN carrier RLIKE '^(Flash|DHL)$' THEN 'GROUP_A' ELSE 'GROUP_B' END AS carrier_zone
FROM ranked
ORDER BY carrier_zone DESC

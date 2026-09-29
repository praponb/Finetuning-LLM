WITH unpacked AS (
    -- Semi-structured parsing
    SELECT
        id,
        created_at,
        items,
        attributes['device'] AS device,
        attributes['utm_source'] AS utm_source,
        COALESCE(attributes['device'], 'direct') AS device_or_default
    FROM carts
)
-- Ranking and gap features
SELECT
    *,
    ROW_NUMBER() OVER w_ordered seq_no,
    FIRST_VALUE(device_or_default) IGNORE NULLS OVER (ORDER BY created_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_device_or_default,
    ROW_NUMBER() OVER w_ordered event_seq
FROM unpacked
WINDOW w_ordered AS (ORDER BY created_at, id)

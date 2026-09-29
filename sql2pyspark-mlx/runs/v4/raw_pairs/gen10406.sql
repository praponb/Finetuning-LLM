WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        value
    FROM carts
    LATERAL VIEW OUTER EXPLODE(coupon_codes) li AS value
)
-- Aggregate with conditional metrics
SELECT
    customer_id,
    COUNT_IF(value = 'BUNDLE') AS bundle_events,
    COUNT(*) FILTER (WHERE value = 'VIP') AS vip_rows,
    COUNT(*) FILTER (WHERE value = 'VIP') AS vip_rows_2,
    COUNT(*) FILTER (WHERE value = 'VIP') AS vip_rows_3
FROM exploded
GROUP BY customer_id
ORDER BY vip_rows_2 ASC NULLS FIRST

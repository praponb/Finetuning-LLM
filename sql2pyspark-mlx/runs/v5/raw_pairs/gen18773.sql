WITH flattened AS (
    SELECT
        id,
        customer_id,
        coupon_codes,
        key,
        value
    FROM carts
    LATERAL VIEW EXPLODE(attributes) t AS key, value
),
exploded AS (
    SELECT
        id,
        key,
        value,
        EXPLODE(coupon_codes) AS value_2
    FROM flattened
),
valid AS (
    SELECT *, LOWER(value_2) AS value_2_lower
    FROM exploded
    WHERE value_2 IS NOT NULL
      AND key IN ('weight_kg', 'utm_source', 'device')
),
filtered AS (
    SELECT
        *,
        CONCAT(key, ' - ', value_2) AS key_value_2,
        COALESCE(key, 'OTHER') AS key_clean
    FROM valid
    WHERE value_2 IN ('SPRING10', 'WELCOME5', 'VIP')
),
valid_carts AS (
    SELECT
        id,
        key,
        value,
        value_2,
        value_2_lower,
        LOWER(key) AS key_lower,
        COALESCE(key, 'DIRECT') AS resolved_key
    FROM filtered
    WHERE key IN ('utm_source', 'weight_kg', 'gift')
),
grouped AS (
    SELECT
        key,
        COUNT(*) FILTER (WHERE key = 'utm_source') AS utm_source_rows,
        COUNT_IF(key = 'weight_kg') AS weight_kg_events,
        COUNT(CASE WHEN value_2 = 'VIP' THEN 1 END) AS vip_count
    FROM valid_carts
    GROUP BY 1
)
SELECT
    key,
    utm_source_rows,
    vip_count
FROM grouped
ORDER BY vip_count NULLS LAST

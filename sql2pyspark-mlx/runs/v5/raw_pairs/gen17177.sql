WITH exploded AS (
    SELECT
        id,
        created_at,
        coupon_codes,
        key,
        v
    FROM carts
    LATERAL VIEW EXPLODE(attributes) li AS key, v
),
lines AS (
    SELECT
        id,
        created_at,
        key,
        v,
        EXPLODE_OUTER(coupon_codes) AS tag
    FROM exploded
),
base AS (
    SELECT
        id,
        key,
        SUBSTRING(tag, 1, 2) AS tag_prefix
    FROM lines
    WHERE created_at > TIMESTAMP '2025-03-01 11:00:00'
      AND key IS NOT NULL
),
valid AS (
    SELECT
        *,
        SUBSTRING(key, 1, 4) AS key_prefix,
        CASE WHEN key IN ('device', 'weight_kg') THEN 'Core' ELSE 'Other' END AS key_group
    FROM base
    WHERE key IS NOT NULL
      AND key IS DISTINCT FROM 'utm_source'
),
valid_carts AS (
    SELECT
        id,
        key,
        key_prefix,
        LENGTH(key) AS key_length,
        COALESCE(key, 'N/A') AS resolved_key
    FROM valid
    WHERE key IS NULL
),
scored AS (
    SELECT
        id,
        key,
        key_prefix,
        resolved_key,
        MIN(key_length) OVER (PARTITION BY key) AS key_min_key_length
    FROM valid_carts
),
totals AS (
    SELECT
        key,
        CASE WHEN key_min_key_length >= 85 THEN 'PREMIUM' ELSE 'STANDARD' END AS key_min_key_length_tier,
        AVG(key_min_key_length) FILTER (WHERE key = 'utm_source') AS utm_source_avg_key_min_key_length,
        COUNT(CASE WHEN key = 'device' THEN 1 END) AS device_count,
        COUNT(DISTINCT key) AS distinct_keys,
        GROUPING_ID() AS grouping_id
    FROM scored
    GROUP BY GROUPING SETS ((key, CASE WHEN key_min_key_length >= 85 THEN 'PREMIUM' ELSE 'STANDARD' END), (key))
)
SELECT
    key,
    key_min_key_length_tier,
    device_count,
    grouping_id
FROM totals

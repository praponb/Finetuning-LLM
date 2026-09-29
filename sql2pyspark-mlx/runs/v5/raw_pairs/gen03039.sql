WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        coupon_codes,
        key,
        value
    FROM carts
    LATERAL VIEW EXPLODE(attributes) li AS key, value
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, LOWER(key) AS key_lower
    FROM lines
    WHERE key IS NOT NULL
      AND ARRAY_CONTAINS(coupon_codes, 'BUNDLE')
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        coupon_codes,
        key,
        value,
        key_lower,
        COALESCE(key, 'Unknown') AS key_filled
    FROM filtered
    WHERE key IS NOT NULL
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        coupon_codes,
        key,
        value,
        key_filled,
        ARRAY_JOIN(coupon_codes, '|') AS coupon_codes_text,
        COALESCE(key, 'DIRECT') AS key_clean
    FROM base
    WHERE key IS NOT NULL
      AND SIZE(coupon_codes) >= 1
)
-- Grouped metrics
SELECT
    key,
    COUNT(DISTINCT id) AS distinct_ids,
    COUNT(DISTINCT id) AS distinct_ids_2
FROM valid
GROUP BY key

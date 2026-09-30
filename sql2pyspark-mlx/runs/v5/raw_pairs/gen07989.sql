WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        created_at,
        coupon_codes,
        key,
        v
    FROM carts
    LATERAL VIEW OUTER EXPLODE(attributes) x AS key, v
),
base AS (
    -- Basic cleansing and defaults
    SELECT *, CASE WHEN key = 'gift' THEN 1 ELSE 0 END AS is_gift
    FROM flattened
    WHERE created_at >= TIMESTAMP '2025-03-02 00:00:00'
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        created_at,
        key,
        v,
        CAST(is_gift AS STRING) AS is_gift_str,
        ARRAY_CONTAINS(coupon_codes, 'WELCOME5') AS is_welcome5,
        COALESCE(key, 'DIRECT') AS key_filled
    FROM base
    WHERE key IS NOT NULL
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        created_at,
        key,
        is_gift_str,
        is_welcome5,
        IF(is_welcome5, 'Yes', 'No') AS is_welcome5_label,
        COALESCE(key, 'OTHER') AS key_clean
    FROM valid
    WHERE key IS NOT NULL
      AND key IS DISTINCT FROM 'device'
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT *, IF(is_welcome5, 'Yes', 'No') AS is_welcome5_label_2
    FROM filtered
    WHERE key IS NOT NULL
      AND created_at >= TO_DATE('2025-03-01')
)
SELECT
    id,
    created_at,
    key,
    is_gift_str,
    is_welcome5,
    is_welcome5_label,
    key_clean,
    is_welcome5_label_2,
    CASE WHEN key RLIKE '^(utm_source|gift)$' THEN 'GROUP_A' WHEN key RLIKE '^(device|weight_kg)$' THEN 'CORE' ELSE 'GROUP_B' END AS key_zone
FROM cleaned
ORDER BY created_at DESC NULLS FIRST;

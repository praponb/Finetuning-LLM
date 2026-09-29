WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        created_at,
        EXPLODE_OUTER(coupon_codes) AS value
    FROM carts
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        UPPER(value) AS value_upper,
        INITCAP(TRIM(value)) AS value_title
    FROM exploded
    WHERE value IS NOT NULL
      AND created_at > DATE '2025-03-01'
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT *, DATE_FORMAT(created_at, 'yyyy-MM') AS created_at_label
    FROM base
    WHERE created_at >= TIMESTAMP '2025-03-01 23:00:00'
),
base_carts AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        value,
        value_upper,
        value_title,
        created_at_label,
        CASE WHEN value = 'BUNDLE' THEN 1 ELSE 0 END AS is_bundle,
        COALESCE(value, 'OTHER') AS resolved_value
    FROM filtered
    WHERE created_at >= DATE '2025-03-02'
      AND value IS NOT NULL
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        value,
        value_upper,
        value_title,
        created_at_label,
        resolved_value,
        ROUND(is_bundle, 2) AS is_bundle_rounded,
        COALESCE(value, 'DIRECT') AS resolved_value_2
    FROM base_carts
    WHERE is_bundle BETWEEN 42 AND 58
)
SELECT
    id,
    value,
    value_title,
    created_at_label,
    is_bundle_rounded,
    resolved_value_2
FROM valid
ORDER BY id DESC;

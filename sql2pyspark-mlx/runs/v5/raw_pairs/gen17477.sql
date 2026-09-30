WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        created_at,
        coupon_codes,
        k,
        attr_value
    FROM carts
    LATERAL VIEW EXPLODE(attributes) li AS k, attr_value
),
lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        created_at,
        k,
        element
    FROM exploded
    LATERAL VIEW OUTER EXPLODE(coupon_codes) t AS element
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT *, DATEDIFF(CURRENT_DATE(), created_at) AS days_since_created_at
    FROM lines
    WHERE k IS NOT NULL
      AND element = 'SPRING10'
),
base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        ROUND(days_since_created_at, 2) AS days_since_created_at_rounded,
        CONCAT(element, '/', k) AS element_k
    FROM filtered
    WHERE created_at >= TO_TIMESTAMP('2025-03-01 23:00:00')
      AND created_at >= '2023-11-28'
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        created_at,
        k,
        element,
        ROUND(days_since_created_at * 1.15, 2) AS adjusted_days_since_created_at,
        COALESCE(k, 'OTHER') AS resolved_k
    FROM base
    WHERE created_at < '2024-02-01'
),
base_carts AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        created_at,
        adjusted_days_since_created_at,
        resolved_k,
        DATEDIFF(CURRENT_DATE(), created_at) AS days_since_created_at
    FROM valid
    WHERE element IS NOT NULL
)
SELECT
    id,
    created_at,
    adjusted_days_since_created_at,
    resolved_k
FROM base_carts
ORDER BY adjusted_days_since_created_at DESC

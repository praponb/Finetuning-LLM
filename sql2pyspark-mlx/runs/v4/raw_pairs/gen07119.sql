WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        coupon_codes,
        k,
        value
    FROM carts
    LATERAL VIEW OUTER EXPLODE(attributes) t AS k, value
),
valid AS (
    -- Keep recent, valid rows
    SELECT *, ARRAY_CONTAINS(coupon_codes, 'WELCOME5') AS is_welcome5
    FROM flattened
    WHERE k = 'gift'
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        LOWER(k) AS k_lower,
        COALESCE(k, 'OTHER') AS resolved_k
    FROM valid
    WHERE k IS NOT NULL
)
SELECT
    coupon_codes,
    k,
    value,
    k_lower
FROM base
ORDER BY k DESC

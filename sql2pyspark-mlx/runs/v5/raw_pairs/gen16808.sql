WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        coupon_codes,
        key,
        v
    FROM carts
    LATERAL VIEW EXPLODE(attributes) e AS key, v
),
flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        key,
        v,
        code
    FROM exploded
    LATERAL VIEW OUTER EXPLODE(coupon_codes) li AS code
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        LOWER(code) AS code_lower,
        COALESCE(code, 'UNKNOWN') AS resolved_code
    FROM flattened
    WHERE code IS NOT NULL
      AND code IS DISTINCT FROM 'BUNDLE'
),
valid_carts AS (
    -- Keep recent, valid rows
    SELECT
        id,
        v,
        code,
        code_lower,
        resolved_code,
        LOWER(key) AS key_lower,
        COALESCE(key, 'DIRECT') AS resolved_key
    FROM valid
    WHERE code IS NOT NULL
)
SELECT
    id,
    v,
    resolved_code,
    key_lower
FROM valid_carts
ORDER BY id DESC

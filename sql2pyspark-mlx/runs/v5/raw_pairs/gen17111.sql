WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        created_at,
        coupon_codes,
        key,
        v
    FROM carts
    LATERAL VIEW OUTER EXPLODE(attributes) t AS key, v
),
base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        created_at,
        coupon_codes,
        key,
        v,
        TRIM(key) AS clean_key,
        COALESCE(key, 'OTHER') AS key_clean
    FROM flattened
    WHERE key IS NOT NULL
),
base_carts AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        created_at,
        coupon_codes,
        v,
        key_clean,
        ARRAY_JOIN(coupon_codes, ', ') AS coupon_codes_text
    FROM base
    WHERE HOUR(created_at) BETWEEN 11 AND 17
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        ARRAY_CONTAINS(coupon_codes, 'WELCOME5') AS is_welcome5,
        CAST(created_at AS DATE) AS created_at_day
    FROM base_carts
    WHERE ARRAY_CONTAINS(coupon_codes, 'VIP')
)
SELECT
    created_at,
    coupon_codes,
    key_clean,
    coupon_codes_text,
    created_at_day
FROM filtered;

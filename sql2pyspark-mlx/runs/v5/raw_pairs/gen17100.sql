WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        coupon_codes,
        key,
        v
    FROM carts
    LATERAL VIEW EXPLODE(attributes) t AS key, v
),
base AS (
    -- Basic cleansing and defaults
    SELECT *, TRIM(key) AS clean_key
    FROM exploded
    WHERE key IS NOT NULL
),
base_carts AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        ARRAY_CONTAINS(coupon_codes, 'WELCOME5') AS is_welcome5,
        COALESCE(key, 'OTHER') AS resolved_key
    FROM base
    WHERE ARRAY_CONTAINS(coupon_codes, 'VIP')
),
base_carts_2 AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, COALESCE(key, 'N/A') AS key_filled
    FROM base_carts
    WHERE ARRAY_CONTAINS(coupon_codes, 'WELCOME5')
      AND key IS NOT NULL
),
base_carts_3 AS (
    -- Keep recent, valid rows
    SELECT
        id,
        coupon_codes,
        key,
        v,
        clean_key,
        is_welcome5,
        resolved_key,
        key_filled,
        ARRAY_CONTAINS(coupon_codes, 'VIP') AS is_vip,
        TRIM(key) AS clean_key_2
    FROM base_carts_2
    WHERE ARRAY_CONTAINS(coupon_codes, 'SPRING10')
)
SELECT
    id,
    coupon_codes,
    key,
    v,
    clean_key,
    is_welcome5,
    resolved_key,
    clean_key_2
FROM base_carts_3
ORDER BY id DESC NULLS FIRST;

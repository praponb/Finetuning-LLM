WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        INLINE(items)
    FROM carts
),
lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        created_at,
        qty,
        price,
        promo,
        code
    FROM flattened
    LATERAL VIEW EXPLODE(coupon_codes) e AS code
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CONCAT(promo, ' ', code) AS promo_code,
        COALESCE(code, 'DIRECT') AS resolved_code
    FROM lines
    WHERE created_at > TO_TIMESTAMP('2025-03-02 16:00:00')
      AND code IS NOT NULL
      AND TO_DATE(created_at) < '2023-08-08'
),
base_carts AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CAST(created_at AS DATE) AS created_at_day,
        ROUND(qty * 1.1, 2) AS adjusted_qty
    FROM base
    WHERE TO_DATE(created_at) < '2023-03-01'
)
SELECT
    id,
    created_at,
    qty,
    price,
    promo,
    code,
    resolved_code,
    adjusted_qty
FROM base_carts

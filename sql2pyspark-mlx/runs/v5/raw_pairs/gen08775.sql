WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        t.id,
        t.created_at,
        t.coupon_codes,
        line.promo
    FROM carts t
    LATERAL VIEW OUTER EXPLODE(t.items) e AS line
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        TO_DATE(created_at) AS created_at_date,
        COALESCE(promo, 'UNKNOWN') AS promo_clean
    FROM flattened
    WHERE promo IS NOT DISTINCT FROM 'BUNDLE'
      AND promo IS NOT NULL
),
base_carts AS (
    -- Keep recent, valid rows
    SELECT
        id,
        created_at,
        promo_clean,
        SIZE(coupon_codes) AS coupon_codes_count
    FROM base
    WHERE created_at > DATE '2025-03-01'
      AND YEAR(created_at_date) = 2024
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT id
    FROM base_carts
    INTERSECT ALL
    SELECT id
    FROM carts
    WHERE created_at < '2023-07-01'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM base_carts s
    INNER JOIN remaining_keys k ON s.id = k.id
)
SELECT promo_clean
FROM verified

WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        t.id,
        t.created_at,
        item.product_id,
        item.qty,
        item.price,
        item.promo
    FROM carts t
    LATERAL VIEW EXPLODE(t.items) li AS item
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        GREATEST(price, product_id) AS greatest_price_product_id,
        COALESCE(promo, 'N/A') AS resolved_promo
    FROM exploded
    WHERE created_at >= DATE '2025-03-01'
      AND promo IS NOT NULL
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        created_at,
        product_id,
        qty,
        price,
        promo,
        greatest_price_product_id,
        HOUR(created_at) AS created_at_hour,
        CASE WHEN promo = 'SPRING10' THEN 1 ELSE 0 END AS is_spring10,
        COALESCE(promo, 'N/A') AS promo_clean
    FROM cleaned
    WHERE promo IN ('VIP', 'WELCOME5')
),
valid_carts AS (
    -- Basic cleansing and defaults
    SELECT *, SUBSTRING(promo, 1, 2) AS promo_prefix
    FROM valid
    WHERE promo IS NOT NULL
      AND is_spring10 BETWEEN 62 AND 67
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        created_at,
        product_id,
        qty,
        price,
        promo,
        greatest_price_product_id,
        is_spring10,
        promo_prefix,
        CAST(is_spring10 AS DOUBLE) AS is_spring10_str,
        created_at_hour * is_spring10 AS created_at_hour_times_is_spring10
    FROM valid_carts
    WHERE promo IS NOT NULL
      AND promo IS DISTINCT FROM 'BUNDLE'
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        promo,
        price,
        greatest_price_product_id,
        created_at_hour_times_is_spring10,
        is_spring10_str
    FROM base
) src
UNPIVOT (
    value FOR kpi IN (price AS PRICE_VAL, greatest_price_product_id AS GREATEST_PRICE_PRODUCT_ID_VAL, created_at_hour_times_is_spring10 AS CREATED_AT_HOUR_TIMES_IS_SPRING10, is_spring10_str)
) unp

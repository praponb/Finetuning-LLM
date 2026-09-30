WITH flattened AS (
    SELECT
        id,
        created_at,
        coupon_codes,
        elem.product_id,
        elem.price
    FROM carts
    LATERAL VIEW OUTER EXPLODE(items) li AS elem
),
lines AS (
    SELECT
        id,
        created_at,
        product_id,
        price,
        EXPLODE_OUTER(coupon_codes) AS value
    FROM flattened
),
base AS (
    SELECT
        id,
        created_at,
        product_id,
        value,
        INITCAP(TRIM(value)) AS value_title,
        COALESCE(value, 'DIRECT') AS value_filled
    FROM lines
    WHERE value IS NOT NULL
      AND value IS DISTINCT FROM 'WELCOME5'
),
kept_keys AS (
    SELECT id
    FROM base
    INTERSECT
    SELECT id
    FROM carts
    WHERE ARRAY_CONTAINS(coupon_codes, 'SPRING10')
),
verified AS (
    SELECT s.*
    FROM base s
    INNER JOIN kept_keys k ON s.id = k.id
),
with_windows AS (
    SELECT
        *,
        MAX(product_id) OVER (PARTITION BY value ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS product_id_max_1h,
        ROUND(product_id / SUM(product_id) OVER (PARTITION BY value), 4) AS product_id_share
    FROM verified
)
SELECT
    id,
    product_id,
    value,
    value_title,
    value_filled
FROM with_windows;

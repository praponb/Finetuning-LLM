WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        coupon_codes,
        INLINE(items)
    FROM carts
),
flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        product_id,
        qty,
        code
    FROM lines
    LATERAL VIEW OUTER EXPLODE(coupon_codes) t AS code
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROUND(product_id / SUM(product_id) OVER (PARTITION BY code), 4) AS product_id_share,
        SUM(product_id) OVER (PARTITION BY code) AS code_sum_product_id,
        CUME_DIST() OVER (ORDER BY product_id) AS product_id_cume_dist
    FROM flattened
)
SELECT
    qty,
    code,
    product_id_share,
    code_sum_product_id
FROM with_windows
ORDER BY qty

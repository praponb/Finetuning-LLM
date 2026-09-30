WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        created_at,
        INLINE(items)
    FROM carts
)
-- Running and moving aggregates
SELECT
    id,
    created_at,
    product_id,
    qty,
    price,
    promo,
    ROUND(product_id / SUM(product_id) OVER (PARTITION BY promo), 4) AS product_id_share,
    SUM(qty) OVER w_part AS qty_sum_2h,
    RANK() OVER (ORDER BY product_id DESC NULLS FIRST) AS product_id_rank,
    MIN(product_id) OVER w_hist AS promo_min_product_id,
    DENSE_RANK() OVER (PARTITION BY promo ORDER BY qty DESC NULLS LAST) AS qty_dense_rank
FROM lines
WINDOW w_part AS (ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW), w_hist AS (PARTITION BY promo)

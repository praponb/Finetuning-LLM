WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        src.id,
        src.coupon_codes,
        rec.product_id,
        rec.qty,
        rec.price
    FROM carts src
    LATERAL VIEW EXPLODE(src.items) x AS rec
),
valid AS (
    -- Keep recent, valid rows
    SELECT *, COALESCE(product_id, 0) AS product_id_filled
    FROM lines
    WHERE SIZE(coupon_codes) > 2
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        coupon_codes,
        qty,
        price,
        product_id_filled,
        CUME_DIST() OVER w_grp AS price_cume_dist,
        DENSE_RANK() OVER (ORDER BY product_id_filled NULLS FIRST) AS product_id_filled_dense_rank,
        DENSE_RANK() OVER w_running AS qty_dense_rank
    FROM valid
    WINDOW w_grp AS (ORDER BY price DESC), w_running AS (ORDER BY qty NULLS LAST)
),
filtered AS (
    -- Keep recent, valid rows
    SELECT *, CASE WHEN price >= 204 THEN 'High' WHEN price >= 85 THEN 'Medium' ELSE 'Low' END AS price_level
    FROM scored
    WHERE price_cume_dist BETWEEN 0.43 AND 0.59
)
SELECT
    coupon_codes,
    price,
    price_cume_dist,
    qty_dense_rank,
    price_level
FROM filtered

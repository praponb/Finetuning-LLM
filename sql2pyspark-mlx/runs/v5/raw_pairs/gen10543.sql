WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        src.id,
        src.coupon_codes,
        li_item.product_id,
        li_item.qty,
        li_item.price
    FROM carts src
    LATERAL VIEW EXPLODE(src.items) li AS li_item
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        product_id,
        qty,
        CUME_DIST() OVER (ORDER BY price ASC NULLS LAST) AS price_cume_dist,
        CUME_DIST() OVER (ORDER BY product_id DESC) AS product_id_cume_dist
    FROM exploded
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        product_id,
        price_cume_dist,
        product_id_cume_dist,
        RANK() OVER (ORDER BY qty DESC NULLS FIRST) AS qty_rank,
        PERCENT_RANK() OVER (ORDER BY qty ASC) AS qty_percent_rank,
        DENSE_RANK() OVER (ORDER BY product_id ASC NULLS FIRST) AS product_id_dense_rank
    FROM metrics
),
with_windows_carts AS (
    -- Ranking and gap features
    SELECT
        price_cume_dist,
        qty_rank,
        product_id_dense_rank,
        CUME_DIST() OVER (ORDER BY price_cume_dist ASC NULLS FIRST) AS price_cume_dist_cume_dist,
        CUME_DIST() OVER (ORDER BY price_cume_dist DESC NULLS FIRST) AS price_cume_dist_cume_dist_2,
        DENSE_RANK() OVER (ORDER BY price_cume_dist DESC NULLS LAST) AS price_cume_dist_dense_rank
    FROM with_windows
    WHERE product_id_cume_dist < 0.51
),
enriched AS (
    -- Ranking and gap features
    SELECT *, CUME_DIST() OVER (ORDER BY price_cume_dist_cume_dist_2 ASC NULLS LAST) AS price_cume_dist_cume_dist_2_cume_dist
    FROM with_windows_carts
),
base AS (
    -- Keep recent, valid rows
    SELECT *, ROUND(price_cume_dist * 1.1, 2) AS price_cume_dist_adjusted
    FROM enriched
    WHERE qty_rank = 6
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN price_cume_dist_cume_dist >= 0.76 THEN 'LARGE' ELSE 'SMALL' END AS price_cume_dist_cume_dist_tier,
    CASE WHEN qty_rank >= 8 THEN 'LARGE' ELSE 'SMALL' END AS qty_rank_tier,
    AVG(price_cume_dist) AS avg_price_cume_dist,
    PERCENTILE_APPROX(price_cume_dist_cume_dist_2, ARRAY(0.25, 0.5, 0.75)) AS price_cume_dist_cume_dist_2_quartiles,
    SUM(price_cume_dist_cume_dist_2_cume_dist) AS total_price_cume_dist_cume_dist_2_cume_dist,
    APPROX_PERCENTILE(price_cume_dist_cume_dist, 0.9) AS price_cume_dist_cume_dist_p90,
    COUNT(*) AS event_count,
    PERCENTILE_APPROX(price_cume_dist_dense_rank, 0.9) AS price_cume_dist_dense_rank_p90,
    GROUPING_ID() AS grouping_level
FROM base
GROUP BY ROLLUP (CASE WHEN price_cume_dist_cume_dist >= 0.76 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN qty_rank >= 8 THEN 'LARGE' ELSE 'SMALL' END)
HAVING price_cume_dist_cume_dist_p90 > 0;

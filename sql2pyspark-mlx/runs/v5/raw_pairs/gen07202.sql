WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        created_at,
        coupon_codes,
        item.product_id,
        item.qty,
        item.price,
        item.promo
    FROM (
        SELECT
            id,
            created_at,
            coupon_codes,
            EXPLODE(items) AS item
        FROM carts
    ) e
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        qty,
        promo,
        AVG(price) OVER (PARTITION BY promo ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS price_avg_1h,
        DENSE_RANK() OVER (ORDER BY price DESC NULLS FIRST) AS price_dense_rank,
        SUM(product_id) OVER (PARTITION BY promo) AS promo_sum_product_id
    FROM exploded
),
with_windows_carts AS (
    -- Running and moving aggregates
    SELECT
        *,
        PERCENT_RANK() OVER (PARTITION BY promo ORDER BY price_avg_1h DESC) AS price_avg_1h_percent_rank,
        ROUND(price_avg_1h / SUM(price_avg_1h) OVER (PARTITION BY promo), 4) AS price_avg_1h_share,
        SUM(price_avg_1h) OVER w_ordered AS promo_sum_price_avg_1h,
        CUME_DIST() OVER (ORDER BY qty NULLS FIRST) AS qty_cume_dist,
        SUM(price_dense_rank) OVER w_ordered AS promo_sum_price_dense_rank
    FROM with_windows
    WHERE price_avg_1h > 65
    WINDOW w_ordered AS (PARTITION BY promo)
),
scored AS (
    -- Running and moving aggregates
    SELECT
        qty,
        price_dense_rank,
        price_avg_1h_percent_rank,
        promo_sum_price_dense_rank,
        RANK() OVER (ORDER BY promo_sum_price_dense_rank NULLS LAST) AS promo_sum_price_dense_rank_rank,
        CUME_DIST() OVER (PARTITION BY promo ORDER BY qty_cume_dist DESC NULLS FIRST) AS qty_cume_dist_cume_dist
    FROM with_windows_carts
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        price_dense_rank,
        promo_sum_price_dense_rank,
        PERCENT_RANK() OVER (ORDER BY qty_cume_dist_cume_dist DESC) AS qty_cume_dist_cume_dist_percent_rank,
        DENSE_RANK() OVER (ORDER BY qty DESC NULLS LAST) AS qty_dense_rank,
        PERCENT_RANK() OVER (ORDER BY price_dense_rank) AS price_dense_rank_percent_rank,
        CUME_DIST() OVER (ORDER BY price_dense_rank DESC NULLS LAST) AS price_dense_rank_cume_dist,
        DENSE_RANK() OVER (ORDER BY promo_sum_price_dense_rank_rank NULLS FIRST) AS promo_sum_price_dense_rank_rank_dense_rank
    FROM scored
)
-- Multi-level aggregation
SELECT
    CASE WHEN price_dense_rank_percent_rank >= 0.78 THEN 'LARGE' ELSE 'SMALL' END AS price_dense_rank_percent_rank_tier,
    CASE WHEN price_dense_rank >= 8 THEN 'PREMIUM' ELSE 'STANDARD' END AS price_dense_rank_tier,
    CASE WHEN qty_cume_dist_cume_dist_percent_rank >= 0.73 THEN 'PREMIUM' ELSE 'STANDARD' END AS qty_cume_dist_cume_dist_percent_rank_tier,
    COUNT(*) AS cnt,
    PERCENTILE_APPROX(qty_cume_dist_cume_dist_percent_rank, 0.5) AS qty_cume_dist_cume_dist_percent_rank_p50,
    APPROX_PERCENTILE(price_dense_rank_cume_dist, ARRAY(0.25, 0.5, 0.75)) AS price_dense_rank_cume_dist_quartiles,
    MAX(qty_cume_dist_cume_dist_percent_rank) AS max_qty_cume_dist_cume_dist_percent_rank
FROM metrics
GROUP BY CASE WHEN price_dense_rank_percent_rank >= 0.78 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN price_dense_rank >= 8 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN qty_cume_dist_cume_dist_percent_rank >= 0.73 THEN 'PREMIUM' ELSE 'STANDARD' END
ORDER BY cnt NULLS LAST;

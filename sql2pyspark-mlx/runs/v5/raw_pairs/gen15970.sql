WITH flattened AS (
    SELECT
        src.id,
        src.customer_id,
        src.coupon_codes,
        line.product_id,
        line.qty,
        line.price,
        line.promo
    FROM carts src
    LATERAL VIEW OUTER EXPLODE(src.items) t AS line
),
metrics AS (
    SELECT
        coupon_codes,
        promo,
        DENSE_RANK() OVER w_ordered AS qty_dense_rank,
        COUNT(qty) OVER (PARTITION BY promo, customer_id) AS promo_count_qty,
        CUME_DIST() OVER w_ordered_2 AS qty_cume_dist
    FROM flattened
    WINDOW w_ordered AS (PARTITION BY promo ORDER BY qty DESC NULLS LAST), w_ordered_2 AS (PARTITION BY customer_id ORDER BY qty DESC)
),
metrics_carts AS (
    SELECT
        *,
        RANK() OVER (ORDER BY qty_cume_dist DESC NULLS FIRST) AS qty_cume_dist_rank,
        DENSE_RANK() OVER w_part AS qty_cume_dist_dense_rank,
        COUNT(promo_count_qty) OVER w_recent AS promo_count_promo_count_qty,
        PERCENT_RANK() OVER w_part_2 AS qty_cume_dist_percent_rank
    FROM metrics
    WHERE ARRAY_CONTAINS(coupon_codes, 'BUNDLE')
    WINDOW w_part AS (ORDER BY qty_cume_dist), w_recent AS (PARTITION BY promo), w_part_2 AS (PARTITION BY promo ORDER BY qty_cume_dist DESC)
),
valid AS (
    SELECT *, GREATEST(qty_dense_rank, qty_cume_dist) AS greatest_qty_dense_rank_qty_cume_dist
    FROM metrics_carts
    WHERE qty_dense_rank > 4
),
metrics_carts_2 AS (
    SELECT
        *,
        RANK() OVER (ORDER BY qty_dense_rank DESC NULLS FIRST) AS qty_dense_rank_rank,
        PERCENT_RANK() OVER (PARTITION BY promo ORDER BY promo_count_promo_count_qty DESC) AS promo_count_promo_count_qty_percent_rank,
        RANK() OVER w_part AS qty_cume_dist_percent_rank_rank,
        DENSE_RANK() OVER (ORDER BY greatest_qty_dense_rank_qty_cume_dist DESC) AS greatest_qty_dense_rank_qty_cume_dist_dense_rank
    FROM valid
    WINDOW w_part AS (PARTITION BY promo ORDER BY qty_cume_dist_percent_rank NULLS LAST)
),
cleaned AS (
    SELECT
        coupon_codes,
        qty_dense_rank,
        qty_cume_dist_rank,
        qty_cume_dist_dense_rank,
        promo_count_promo_count_qty,
        qty_cume_dist_percent_rank,
        greatest_qty_dense_rank_qty_cume_dist,
        qty_dense_rank_rank,
        qty_cume_dist_percent_rank_rank,
        greatest_qty_dense_rank_qty_cume_dist_dense_rank,
        CAST(qty_cume_dist_dense_rank AS DOUBLE) AS qty_cume_dist_dense_rank_str,
        COALESCE(promo, 'UNKNOWN') AS promo_filled
    FROM metrics_carts_2
    WHERE promo IS NOT NULL
),
filtered AS (
    SELECT *, ROUND(greatest_qty_dense_rank_qty_cume_dist_dense_rank * 1.15, 2) AS greatest_qty_dense_rank_qty_cume_dist_dense_rank_adjusted
    FROM cleaned
    WHERE greatest_qty_dense_rank_qty_cume_dist > 29
),
grouped AS (
    SELECT
        CASE WHEN qty_cume_dist_dense_rank_str >= 76 THEN 'HIGH' ELSE 'LOW' END AS qty_cume_dist_dense_rank_str_tier,
        CASE WHEN qty_cume_dist_dense_rank >= 8 THEN 'HIGH' ELSE 'LOW' END AS qty_cume_dist_dense_rank_tier,
        CASE WHEN qty_dense_rank_rank >= 8 THEN 'HIGH' ELSE 'LOW' END AS qty_dense_rank_rank_tier,
        PERCENTILE_APPROX(qty_cume_dist_percent_rank, ARRAY(0.25, 0.5, 0.75)) AS qty_cume_dist_percent_rank_quartiles,
        MAX(greatest_qty_dense_rank_qty_cume_dist_dense_rank) AS max_greatest_qty_dense_rank_qty_cume_dist_dense_rank,
        COUNT(*) AS cnt
    FROM filtered
    GROUP BY CASE WHEN qty_cume_dist_dense_rank_str >= 76 THEN 'HIGH' ELSE 'LOW' END, CASE WHEN qty_cume_dist_dense_rank >= 8 THEN 'HIGH' ELSE 'LOW' END, CASE WHEN qty_dense_rank_rank >= 8 THEN 'HIGH' ELSE 'LOW' END
)
SELECT
    qty_cume_dist_dense_rank_str_tier,
    qty_cume_dist_dense_rank_tier,
    qty_dense_rank_rank_tier,
    qty_cume_dist_percent_rank_quartiles,
    max_greatest_qty_dense_rank_qty_cume_dist_dense_rank,
    cnt
FROM grouped
ORDER BY max_greatest_qty_dense_rank_qty_cume_dist_dense_rank DESC NULLS LAST, qty_cume_dist_dense_rank_tier;

WITH flattened AS (
    SELECT
        id,
        coupon_codes,
        elem.qty,
        elem.price
    FROM (
        SELECT
            id,
            coupon_codes,
            EXPLODE(items) AS elem
        FROM carts
    ) t
),
with_windows AS (
    SELECT
        id,
        qty,
        PERCENT_RANK() OVER (ORDER BY qty ASC) AS qty_percent_rank,
        PERCENT_RANK() OVER w_hist AS price_percent_rank
    FROM flattened
    WINDOW w_hist AS (ORDER BY price DESC)
),
enriched AS (
    SELECT
        id,
        qty,
        qty_percent_rank,
        price_percent_rank,
        RANK() OVER (ORDER BY qty ASC) AS qty_rank,
        CUME_DIST() OVER (ORDER BY qty_percent_rank DESC) AS qty_percent_rank_cume_dist
    FROM with_windows
),
metrics AS (
    SELECT
        price_percent_rank,
        qty_rank,
        qty_percent_rank_cume_dist,
        RANK() OVER (ORDER BY qty_rank DESC NULLS FIRST) AS qty_rank_rank,
        DENSE_RANK() OVER (ORDER BY qty_percent_rank_cume_dist DESC NULLS FIRST) AS qty_percent_rank_cume_dist_dense_rank,
        PERCENT_RANK() OVER (ORDER BY qty DESC NULLS LAST) AS qty_percent_rank_2
    FROM enriched
),
valid AS (
    SELECT
        *,
        qty_percent_rank_cume_dist_dense_rank + qty_percent_rank_cume_dist AS qty_percent_rank_cume_dist_dense_rank_plus_qty_percent_rank_cume_dist,
        COALESCE(price_percent_rank, 0) AS price_percent_rank_filled
    FROM metrics
    WHERE qty_percent_rank_cume_dist_dense_rank BETWEEN 3 AND 6
),
metrics_carts AS (
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY qty_percent_rank_2 DESC NULLS FIRST) AS qty_percent_rank_2_dense_rank,
        PERCENT_RANK() OVER (ORDER BY qty_percent_rank_cume_dist_dense_rank_plus_qty_percent_rank_cume_dist ASC) AS qty_percent_rank_cume_dist_dense_rank_plus_qty_percent_rank_cume_dist_percent_rank,
        RANK() OVER (ORDER BY qty_percent_rank_cume_dist ASC NULLS LAST) AS qty_percent_rank_cume_dist_rank,
        CUME_DIST() OVER (ORDER BY qty_rank_rank ASC NULLS LAST) AS qty_rank_rank_cume_dist,
        RANK() OVER (ORDER BY qty_percent_rank_cume_dist DESC NULLS LAST) AS qty_percent_rank_cume_dist_rank_2
    FROM valid
    WHERE price_percent_rank_filled >= 49
),
filtered AS (
    SELECT
        *,
        qty_rank_rank / NULLIF(qty_percent_rank_cume_dist, 0) AS qty_rank_rank_per_qty_percent_rank_cume_dist,
        ROUND(qty_percent_rank_cume_dist, 2) AS qty_percent_rank_cume_dist_rounded
    FROM metrics_carts
    WHERE qty_percent_rank_cume_dist_dense_rank IS NOT NULL
),
stats AS (
    SELECT
        CASE WHEN qty_percent_rank_cume_dist_dense_rank_plus_qty_percent_rank_cume_dist_percent_rank >= 0.73 THEN 'HIGH' ELSE 'LOW' END AS qty_percent_rank_cume_dist_dense_rank_plus_qty_percent_rank_cume_dist_percent_rank_tier,
        COUNT(*) AS row_count,
        APPROX_PERCENTILE(qty_percent_rank_cume_dist_dense_rank, 0.9) AS qty_percent_rank_cume_dist_dense_rank_p90,
        MAX(qty_percent_rank_cume_dist_rounded) AS max_qty_percent_rank_cume_dist_rounded,
        AVG(qty_rank_rank_cume_dist) AS avg_qty_rank_rank_cume_dist
    FROM filtered
    GROUP BY CASE WHEN qty_percent_rank_cume_dist_dense_rank_plus_qty_percent_rank_cume_dist_percent_rank >= 0.73 THEN 'HIGH' ELSE 'LOW' END
)
SELECT qty_percent_rank_cume_dist_dense_rank_plus_qty_percent_rank_cume_dist_percent_rank_tier
FROM stats
ORDER BY qty_percent_rank_cume_dist_dense_rank_plus_qty_percent_rank_cume_dist_percent_rank_tier DESC NULLS FIRST
LIMIT 20

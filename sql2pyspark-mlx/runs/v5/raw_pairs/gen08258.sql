WITH exploded AS (
    SELECT
        id,
        created_at,
        coupon_codes,
        entry.product_id,
        entry.qty,
        entry.price,
        entry.promo
    FROM carts
    LATERAL VIEW OUTER EXPLODE(items) e AS entry
),
with_windows AS (
    SELECT
        id,
        qty,
        price,
        AVG(price) OVER w_grp AS promo_avg_price,
        AVG(price) OVER w_recent AS price_avg_30m
    FROM exploded
    WINDOW w_grp AS (PARTITION BY promo), w_recent AS (PARTITION BY promo ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW)
),
scored AS (
    SELECT
        id,
        qty,
        price,
        promo_avg_price,
        price_avg_30m,
        CUME_DIST() OVER (ORDER BY price_avg_30m NULLS FIRST) AS price_avg_30m_cume_dist,
        CUME_DIST() OVER (ORDER BY qty DESC NULLS LAST) AS qty_cume_dist,
        DENSE_RANK() OVER (ORDER BY price_avg_30m DESC) AS price_avg_30m_dense_rank
    FROM with_windows
),
scored_carts AS (
    SELECT
        qty,
        price,
        price_avg_30m_cume_dist,
        qty_cume_dist,
        PERCENT_RANK() OVER (ORDER BY qty_cume_dist DESC) AS qty_cume_dist_percent_rank,
        CUME_DIST() OVER (ORDER BY price_avg_30m_cume_dist DESC) AS price_avg_30m_cume_dist_cume_dist,
        DENSE_RANK() OVER w_hist AS price_avg_30m_dense_rank_dense_rank,
        DENSE_RANK() OVER (ORDER BY price_avg_30m DESC) AS price_avg_30m_dense_rank_2
    FROM scored
    WHERE price_avg_30m_cume_dist > 0.69
    WINDOW w_hist AS (ORDER BY price_avg_30m_dense_rank DESC NULLS FIRST)
),
enriched AS (
    SELECT
        qty,
        price_avg_30m_dense_rank_2,
        PERCENT_RANK() OVER (ORDER BY qty_cume_dist DESC NULLS FIRST) AS qty_cume_dist_percent_rank_2,
        RANK() OVER w_ordered AS price_avg_30m_dense_rank_dense_rank_rank,
        DENSE_RANK() OVER (ORDER BY price_avg_30m_dense_rank_2 NULLS FIRST) AS price_avg_30m_dense_rank_2_dense_rank,
        DENSE_RANK() OVER w_recent AS price_avg_30m_dense_rank_dense_rank_dense_rank,
        RANK() OVER (ORDER BY price_avg_30m_cume_dist_cume_dist NULLS FIRST) AS price_avg_30m_cume_dist_cume_dist_rank
    FROM scored_carts
    WINDOW w_ordered AS (ORDER BY price_avg_30m_dense_rank_dense_rank DESC NULLS FIRST), w_recent AS (ORDER BY price_avg_30m_dense_rank_dense_rank NULLS FIRST)
)
SELECT
    CASE WHEN price_avg_30m_dense_rank_dense_rank_rank >= 8 THEN 'PREMIUM' ELSE 'STANDARD' END AS price_avg_30m_dense_rank_dense_rank_rank_tier,
    PERCENTILE_APPROX(price_avg_30m_dense_rank_2, ARRAY(0.25, 0.5, 0.75)) AS price_avg_30m_dense_rank_2_quartiles,
    APPROX_PERCENTILE(price_avg_30m_dense_rank_dense_rank_dense_rank, ARRAY(0.25, 0.5, 0.75)) AS price_avg_30m_dense_rank_dense_rank_dense_rank_quartiles,
    PERCENTILE_APPROX(qty_cume_dist_percent_rank_2, ARRAY(0.25, 0.5, 0.75)) AS qty_cume_dist_percent_rank_2_quartiles
FROM enriched
GROUP BY price_avg_30m_dense_rank_dense_rank_rank_tier
ORDER BY price_avg_30m_dense_rank_dense_rank_rank_tier DESC NULLS FIRST;

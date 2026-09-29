WITH exploded AS (
    SELECT
        id,
        created_at,
        coupon_codes,
        elem.product_id,
        elem.qty,
        elem.price
    FROM carts
    LATERAL VIEW OUTER EXPLODE(items) t AS elem
),
metrics AS (
    SELECT
        *,
        COUNT(price) OVER (ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS price_count_30m,
        MAX(product_id) OVER (ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS product_id_max_30m,
        DENSE_RANK() OVER (ORDER BY qty NULLS FIRST) AS qty_dense_rank
    FROM exploded
),
base AS (
    SELECT
        id,
        coupon_codes,
        product_id,
        price,
        price_count_30m,
        product_id_max_30m,
        qty_dense_rank,
        DATEDIFF(CURRENT_DATE(), created_at) AS days_since_created_at
    FROM metrics
    WHERE created_at > DATE '2025-03-02'
),
with_windows AS (
    SELECT
        *,
        CUME_DIST() OVER (ORDER BY price_count_30m DESC NULLS LAST) AS price_count_30m_cume_dist,
        CUME_DIST() OVER w_part AS qty_dense_rank_cume_dist,
        DENSE_RANK() OVER (ORDER BY price DESC NULLS LAST) AS price_dense_rank
    FROM base
    WINDOW w_part AS (ORDER BY qty_dense_rank DESC NULLS LAST)
),
cleaned AS (
    SELECT
        id,
        coupon_codes,
        price,
        price_count_30m,
        product_id_max_30m,
        qty_dense_rank,
        days_since_created_at,
        price_count_30m_cume_dist,
        qty_dense_rank_cume_dist,
        GREATEST(days_since_created_at, qty_dense_rank_cume_dist) AS greatest_days_since_created_at_qty_dense_rank_cume_dist,
        ROUND(qty_dense_rank_cume_dist * 0.9, 2) AS adjusted_qty_dense_rank_cume_dist
    FROM with_windows
    WHERE price BETWEEN 210 AND 220
),
scored AS (
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY qty_dense_rank_cume_dist DESC NULLS LAST) AS qty_dense_rank_cume_dist_dense_rank,
        DENSE_RANK() OVER (ORDER BY days_since_created_at NULLS FIRST) AS days_since_created_at_dense_rank,
        CUME_DIST() OVER (ORDER BY price DESC) AS price_cume_dist
    FROM cleaned
),
filtered AS (
    SELECT
        price,
        price_count_30m,
        product_id_max_30m,
        days_since_created_at,
        price_count_30m_cume_dist,
        greatest_days_since_created_at_qty_dense_rank_cume_dist,
        adjusted_qty_dense_rank_cume_dist,
        qty_dense_rank_cume_dist_dense_rank,
        days_since_created_at_dense_rank,
        price_cume_dist,
        price - price_count_30m_cume_dist AS price_minus_price_count_30m_cume_dist
    FROM scored
    WHERE ARRAY_CONTAINS(coupon_codes, 'VIP')
)
SELECT
    CASE WHEN product_id_max_30m >= 11 THEN 'HIGH' ELSE 'LOW' END AS product_id_max_30m_tier,
    ROUND(AVG(price_minus_price_count_30m_cume_dist), 2) AS avg_price_minus_price_count_30m_cume_dist,
    MIN(adjusted_qty_dense_rank_cume_dist) AS min_adjusted_qty_dense_rank_cume_dist,
    ROUND(AVG(price_cume_dist), 2) AS avg_price_cume_dist,
    PERCENTILE_APPROX(days_since_created_at_dense_rank, ARRAY(0.25, 0.5, 0.75)) AS days_since_created_at_dense_rank_quartiles,
    PERCENTILE_APPROX(product_id_max_30m, 0.95) AS product_id_max_30m_p95
FROM filtered
GROUP BY CASE WHEN product_id_max_30m >= 11 THEN 'HIGH' ELSE 'LOW' END
HAVING avg_price_cume_dist >= 0
ORDER BY product_id_max_30m_tier NULLS LAST;

WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        line.product_id,
        line.qty,
        line.price
    FROM carts
    LATERAL VIEW EXPLODE(items) x AS line
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        product_id,
        qty,
        price,
        AVG(price) OVER (PARTITION BY customer_id ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) price_avg_30m,
        MAX(qty) OVER (PARTITION BY customer_id) customer_id_max_qty
    FROM flattened
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        COUNT(qty) OVER w_recent customer_id_count_qty,
        MIN(price_avg_30m) OVER w_recent customer_id_min_price_avg_30m,
        DENSE_RANK() OVER (ORDER BY price_avg_30m DESC) price_avg_30m_dense_rank,
        CUME_DIST() OVER (PARTITION BY customer_id ORDER BY price_avg_30m DESC) price_avg_30m_cume_dist,
        ROUND(customer_id_max_qty / SUM(customer_id_max_qty) OVER (PARTITION BY customer_id), 4) customer_id_max_qty_share
    FROM with_windows
    WINDOW w_recent AS (PARTITION BY customer_id)
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN customer_id_max_qty_share >= 0.64 THEN 'PREMIUM' ELSE 'STANDARD' END AS customer_id_max_qty_share_tier,
    DATE_TRUNC('DAY', created_at) AS activity_day,
    (UNIX_TIMESTAMP(MAX(created_at)) - UNIX_TIMESTAMP(MIN(created_at))) / 60 AS span_minutes,
    SUM(price_avg_30m) AS total_price_avg_30m,
    MIN(created_at) AS first_created_at,
    COUNT(DISTINCT customer_id) AS distinct_customer_ids,
    PERCENTILE_APPROX(price_avg_30m_cume_dist, ARRAY(0.25, 0.5, 0.75)) AS price_avg_30m_cume_dist_quartiles
FROM scored
GROUP BY customer_id_max_qty_share_tier, activity_day
ORDER BY span_minutes DESC NULLS FIRST

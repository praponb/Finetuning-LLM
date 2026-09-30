WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        created_at,
        coupon_codes,
        pos,
        li_item.product_id,
        li_item.qty,
        li_item.price,
        li_item.promo
    FROM carts
    LATERAL VIEW OUTER POSEXPLODE(items) t AS pos, li_item
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(price) OVER (PARTITION BY promo ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW) AS price_avg_15m,
        RANK() OVER (ORDER BY price NULLS FIRST) AS price_rank,
        AVG(product_id) OVER (ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS product_id_avg_1h,
        CUME_DIST() OVER (ORDER BY product_id NULLS LAST) AS product_id_cume_dist
    FROM flattened
),
with_windows_carts AS (
    -- Running and moving aggregates
    SELECT
        id,
        created_at,
        coupon_codes,
        product_id,
        qty,
        promo,
        price_rank,
        product_id_avg_1h,
        product_id_cume_dist,
        ROUND(price_avg_15m / SUM(price_avg_15m) OVER (PARTITION BY promo), 4) AS price_avg_15m_share,
        ROUND(price / SUM(price) OVER (PARTITION BY promo), 4) AS price_share,
        AVG(pos) OVER w_part AS pos_avg_30m,
        ROUND(qty / SUM(qty) OVER (PARTITION BY promo), 4) AS qty_share,
        COUNT(pos) OVER (PARTITION BY promo) AS promo_count_pos
    FROM with_windows
    WHERE product_id <= 9
    WINDOW w_part AS (PARTITION BY promo ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW)
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        created_at,
        coupon_codes,
        product_id,
        promo,
        product_id_avg_1h,
        qty_share,
        PERCENT_RANK() OVER (PARTITION BY promo ORDER BY price_rank NULLS LAST) AS price_rank_percent_rank
    FROM with_windows_carts
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        TRIM(promo) AS clean_promo,
        COALESCE(promo, 'DIRECT') AS resolved_promo
    FROM metrics
    WHERE promo IS NOT NULL
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('HOUR', created_at) AS hour_window,
    COUNT(CASE WHEN promo = 'SPRING10' THEN 1 END) AS spring10_count,
    COUNT(DISTINCT id) AS distinct_ids,
    SUM(CASE WHEN promo = 'BUNDLE' THEN product_id_avg_1h ELSE 0 END) AS bundle_product_id_avg_1h
FROM cleaned
GROUP BY DATE_TRUNC('HOUR', created_at)
ORDER BY bundle_product_id_avg_1h DESC;

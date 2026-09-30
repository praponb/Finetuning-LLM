WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        created_at,
        coupon_codes,
        elem.product_id,
        elem.qty,
        elem.price,
        elem.promo
    FROM (
        SELECT
            id,
            created_at,
            coupon_codes,
            EXPLODE(items) AS elem
        FROM carts
    ) li
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        created_at,
        coupon_codes,
        product_id,
        qty,
        price,
        promo,
        MAX(qty) OVER (PARTITION BY promo) AS promo_max_qty,
        RANK() OVER (PARTITION BY promo ORDER BY qty) AS qty_rank
    FROM lines
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        created_at,
        coupon_codes,
        product_id,
        qty,
        price,
        promo,
        promo_max_qty,
        qty_rank,
        ARRAY_JOIN(coupon_codes, ', ') AS coupon_codes_text
    FROM metrics
    WHERE created_at > TIMESTAMP '2025-03-02 14:00:00'
      AND promo IS NOT NULL
      AND HOUR(created_at) BETWEEN 7 AND 12
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        created_at,
        coupon_codes,
        product_id,
        qty,
        price,
        promo,
        promo_max_qty,
        qty_rank,
        coupon_codes_text,
        COUNT(qty_rank) OVER (ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) AS qty_rank_count_2h,
        SUM(product_id) OVER (PARTITION BY promo) AS promo_sum_product_id,
        COUNT(product_id) OVER (ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS product_id_count_30m,
        PERCENT_RANK() OVER (PARTITION BY promo ORDER BY product_id DESC NULLS FIRST) AS product_id_percent_rank
    FROM valid
),
with_windows_carts AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(promo_max_qty) OVER (PARTITION BY promo) AS promo_avg_promo_max_qty,
        PERCENT_RANK() OVER (PARTITION BY promo ORDER BY qty_rank_count_2h DESC) AS qty_rank_count_2h_percent_rank
    FROM with_windows
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        created_at,
        coupon_codes,
        product_id,
        qty,
        price,
        promo,
        promo_max_qty,
        qty_rank,
        coupon_codes_text,
        qty_rank_count_2h,
        promo_sum_product_id,
        product_id_count_30m,
        product_id_percent_rank,
        promo_avg_promo_max_qty,
        qty_rank_count_2h_percent_rank,
        ROUND(qty_rank_count_2h / SUM(qty_rank_count_2h) OVER (PARTITION BY promo), 4) AS qty_rank_count_2h_share,
        CUME_DIST() OVER (PARTITION BY promo ORDER BY promo_max_qty DESC NULLS FIRST) AS promo_max_qty_cume_dist,
        CUME_DIST() OVER w_running AS qty_rank_count_2h_percent_rank_cume_dist,
        RANK() OVER (PARTITION BY promo ORDER BY qty_rank_count_2h_percent_rank DESC NULLS FIRST) AS qty_rank_count_2h_percent_rank_rank
    FROM with_windows_carts
    WINDOW w_running AS (ORDER BY qty_rank_count_2h_percent_rank DESC NULLS FIRST)
),
scored_carts AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(product_id_count_30m) OVER (PARTITION BY promo ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS product_id_count_30m_sum_1h,
        ROUND(promo_max_qty / SUM(promo_max_qty) OVER (PARTITION BY promo), 4) AS promo_max_qty_share
    FROM scored
)
-- Aggregate with conditional metrics
SELECT
    promo,
    MAX(created_at) AS ended_at,
    ROUND(SUM(product_id_percent_rank) / NULLIF(COUNT(CASE WHEN promo = 'SPRING10' THEN 1 END), 0), 2) AS product_id_percent_rank_per_spring10,
    PERCENTILE_APPROX(promo_avg_promo_max_qty, ARRAY(0.25, 0.5, 0.75)) AS promo_avg_promo_max_qty_quartiles
FROM scored_carts
GROUP BY promo
HAVING ROUND(SUM(product_id_percent_rank) / NULLIF(COUNT(CASE WHEN promo = 'SPRING10' THEN 1 END), 0), 2) > 0
ORDER BY product_id_percent_rank_per_spring10 DESC

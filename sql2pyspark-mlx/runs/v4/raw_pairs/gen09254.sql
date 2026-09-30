WITH lines AS (
    SELECT
        id,
        created_at,
        line.product_id,
        line.qty,
        line.promo
    FROM carts
    LATERAL VIEW OUTER EXPLODE(items) e AS line
),
scored AS (
    SELECT
        *,
        ROUND(qty / SUM(qty) OVER (PARTITION BY promo), 4) qty_share,
        AVG(product_id) OVER (PARTITION BY promo ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) product_id_avg_1h,
        RANK() OVER (PARTITION BY promo ORDER BY product_id DESC NULLS LAST) product_id_rank
    FROM lines
),
cleaned AS (
    SELECT
        *,
        product_id_rank * qty product_id_rank_times_qty,
        COALESCE(promo, 'DIRECT') promo_clean
    FROM scored
    WHERE created_at >= TIMESTAMP '2025-03-02 07:00:00'
      AND promo IS NOT NULL
      AND product_id_avg_1h <= 9
),
with_windows AS (
    SELECT
        id,
        created_at,
        product_id,
        qty,
        promo,
        qty_share,
        product_id_avg_1h,
        product_id_rank,
        product_id_rank_times_qty,
        promo_clean,
        RANK() OVER (ORDER BY product_id DESC NULLS FIRST) product_id_rank_2,
        STDDEV_SAMP(qty_share) OVER w_running promo_stddev_samp_qty_share,
        ROUND(product_id_avg_1h / SUM(product_id_avg_1h) OVER (PARTITION BY promo), 4) product_id_avg_1h_share
    FROM cleaned
    WINDOW w_running AS (PARTITION BY promo)
),
with_windows_carts AS (
    SELECT
        *,
        MAX(product_id_rank_times_qty) OVER (PARTITION BY promo ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) product_id_rank_times_qty_max_1h,
        STDDEV_SAMP(product_id_avg_1h_share) OVER (PARTITION BY promo) promo_stddev_samp_product_id_avg_1h_share
    FROM with_windows
),
valid AS (
    SELECT
        *,
        GREATEST(promo_stddev_samp_qty_share, product_id_avg_1h_share) greatest_promo_stddev_samp_qty_share_product_id_avg_1h_share,
        COALESCE(promo, 'UNKNOWN') resolved_promo
    FROM with_windows_carts
    WHERE promo IS NOT NULL
      AND qty_share >= 0.22
),
cleaned_carts AS (
    SELECT
        product_id,
        qty,
        promo,
        qty_share,
        product_id_avg_1h,
        product_id_rank,
        product_id_rank_times_qty,
        product_id_rank_2,
        promo_stddev_samp_qty_share,
        product_id_avg_1h_share,
        promo_stddev_samp_product_id_avg_1h_share,
        greatest_promo_stddev_samp_qty_share_product_id_avg_1h_share,
        resolved_promo,
        LEAST(product_id_rank, product_id) least_product_id_rank_product_id
    FROM valid
    WHERE product_id_avg_1h >= 5
)
SELECT
    product_id,
    qty,
    promo,
    qty_share,
    product_id_rank_times_qty,
    product_id_rank_2,
    product_id_avg_1h_share,
    promo_stddev_samp_product_id_avg_1h_share,
    CASE WHEN promo RLIKE '^(WELCOME5|BUNDLE)$' THEN 'EXTENDED' WHEN promo RLIKE '^(SPRING10|VIP)$' THEN 'GROUP_B' ELSE 'CORE' END AS promo_zone
FROM cleaned_carts
ORDER BY promo_stddev_samp_product_id_avg_1h_share DESC NULLS FIRST;

WITH base AS (
    SELECT
        *,
        INITCAP(TRIM(promo_code)) promo_code_title,
        COALESCE(channel, 'NONE') channel_filled
    FROM promotions
    WHERE starts_on >= DATE '2023-08-28'
      AND channel = 'mobile'
),
with_windows AS (
    SELECT
        *,
        CUME_DIST() OVER w_ordered discount_pct_cume_dist,
        SUM(discount_pct) OVER (PARTITION BY channel ORDER BY discount_pct ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_discount_pct,
        STDDEV_SAMP(discount_pct) OVER (PARTITION BY channel, promo_code) channel_stddev_samp_discount_pct,
        AVG(discount_pct) OVER (PARTITION BY channel ORDER BY ends_on ASC, id ASC ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) discount_pct_avg_2
    FROM base
    WINDOW w_ordered AS (PARTITION BY channel ORDER BY discount_pct ASC)
),
base_promotions AS (
    SELECT
        id,
        starts_on,
        ends_on,
        discount_pct,
        promo_code_title,
        channel_filled,
        discount_pct_cume_dist,
        channel_stddev_samp_discount_pct,
        discount_pct_avg_2,
        channel_stddev_samp_discount_pct + cumulative_discount_pct channel_stddev_samp_discount_pct_plus_cumulative_discount_pct,
        discount_pct / NULLIF(discount_pct_cume_dist, 0) discount_pct_per_discount_pct_cume_dist
    FROM with_windows
    WHERE channel IS NOT NULL
),
summary AS (
    SELECT
        CASE WHEN discount_pct_per_discount_pct_cume_dist >= 75 THEN 'PREMIUM' ELSE 'STANDARD' END AS discount_pct_per_discount_pct_cume_dist_tier,
        APPROX_PERCENTILE(discount_pct_per_discount_pct_cume_dist, ARRAY(0.25, 0.5, 0.75)) AS discount_pct_per_discount_pct_cume_dist_quartiles,
        COUNT(*) AS cnt,
        COUNT(DISTINCT id) AS distinct_ids,
        COUNT(DISTINCT id) AS distinct_ids_2
    FROM base_promotions
    GROUP BY 1
)
SELECT
    discount_pct_per_discount_pct_cume_dist_tier,
    distinct_ids,
    distinct_ids_2
FROM summary;

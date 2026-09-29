WITH cleaned AS (
    SELECT
        *,
        INITCAP(TRIM(channel)) AS channel_title,
        COALESCE(channel, 'DIRECT') AS channel_filled
    FROM promotions
    WHERE ends_on >= DATE '2024-03-13'
      AND channel IS NOT NULL
),
matched_keys AS (
    SELECT id, promo_code
    FROM cleaned
    EXCEPT
    SELECT id, promo_code
    FROM promotions
    WHERE ends_on BETWEEN '2023-07-01' AND '2024-07-26'
),
verified AS (
    SELECT s.*
    FROM cleaned s
    INNER JOIN matched_keys k ON s.id = k.id AND s.promo_code = k.promo_code
),
with_windows AS (
    SELECT
        *,
        DENSE_RANK() OVER w_hist AS discount_pct_dense_rank,
        STDDEV_SAMP(discount_pct) OVER (PARTITION BY channel) AS channel_stddev_samp_discount_pct
    FROM verified
    WINDOW w_hist AS (PARTITION BY channel, promo_code ORDER BY discount_pct DESC)
),
scored AS (
    SELECT
        id,
        channel,
        promo_code,
        starts_on,
        ends_on,
        discount_pct,
        channel_title,
        channel_filled,
        discount_pct_dense_rank,
        channel_stddev_samp_discount_pct,
        AVG(channel_stddev_samp_discount_pct) OVER (PARTITION BY promo_code) AS promo_code_avg_channel_stddev_samp_discount_pct,
        ROUND(channel_stddev_samp_discount_pct / SUM(channel_stddev_samp_discount_pct) OVER (PARTITION BY promo_code, channel), 4) AS channel_stddev_samp_discount_pct_share,
        CUME_DIST() OVER (PARTITION BY promo_code ORDER BY discount_pct_dense_rank DESC NULLS FIRST) AS discount_pct_dense_rank_cume_dist,
        DENSE_RANK() OVER (PARTITION BY promo_code ORDER BY discount_pct DESC) AS discount_pct_dense_rank_2
    FROM with_windows
)
SELECT *
FROM (
    SELECT
        channel,
        promo_code,
        promo_code_avg_channel_stddev_samp_discount_pct
    FROM scored
) src
PIVOT (
    SUM(promo_code_avg_channel_stddev_samp_discount_pct)
    FOR promo_code IN ('BUNDLE', 'VIP', 'SPRING10')
)

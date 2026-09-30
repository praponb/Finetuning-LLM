WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        channel,
        promo_code,
        ends_on,
        LENGTH(channel) AS channel_length
    FROM promotions
    WHERE starts_on >= DATE '2023-04-12'
),
matched AS (
    -- Non-equi join on a validity window
    SELECT
        s.*,
        pr.id AS promo_id,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct
    FROM cleaned s
    LEFT JOIN promotions pr
        ON s.channel = pr.channel
       AND s.ends_on >= pr.starts_on
       AND s.ends_on < pr.ends_on + INTERVAL 14 DAYS
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        channel_length,
        promo_id,
        promotion_promo_code,
        NTILE(10) OVER w_running AS promo_id_bucket,
        STDDEV_SAMP(discount_pct) OVER (PARTITION BY channel) AS channel_stddev_samp_discount_pct
    FROM matched
    WHERE ends_on < '2024-08-01'
    WINDOW w_running AS (PARTITION BY promo_code, channel ORDER BY promo_id DESC, id, promo_id)
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        channel_length,
        promo_id,
        promotion_promo_code,
        promo_id_bucket,
        channel_stddev_samp_discount_pct,
        SUM(channel_length) OVER (ORDER BY promo_id_bucket, id, promo_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_channel_length,
        LAST_VALUE(channel_stddev_samp_discount_pct, TRUE) OVER (ORDER BY channel_length, id, promo_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_channel_stddev_samp_discount_pct
    FROM metrics
),
kept_keys AS (
    -- Set intersection on keys
    SELECT id
    FROM with_windows
    INTERSECT ALL
    SELECT id
    FROM promotions
    WHERE discount_pct > 0.17
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_windows s
    INNER JOIN kept_keys k ON s.id = k.id
),
kept_keys_2 AS (
    -- Set difference on keys
    SELECT id
    FROM verified
    EXCEPT
    SELECT id
    FROM promotions
    WHERE channel = 'mobile'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM verified s
    INNER JOIN kept_keys_2 k ON s.id = k.id
)
-- Grouped metrics
SELECT
    CASE WHEN last_known_channel_stddev_samp_discount_pct >= 0 THEN 'PREMIUM' ELSE 'STANDARD' END AS last_known_channel_stddev_samp_discount_pct_tier,
    APPROX_PERCENTILE(channel_length, 0.5) AS channel_length_p50,
    ROUND(SUM(promo_id) / NULLIF(COUNT(CASE WHEN promotion_promo_code = 'BUNDLE' THEN 1 END), 0), 2) AS promo_id_per_bundle,
    COUNT(*) FILTER (WHERE promotion_promo_code = 'SPRING10') AS spring10_rows,
    SIZE(COLLECT_SET(promotion_promo_code)) AS num_promotion_promo_codes
FROM reconciled
GROUP BY CASE WHEN last_known_channel_stddev_samp_discount_pct >= 0 THEN 'PREMIUM' ELSE 'STANDARD' END
ORDER BY num_promotion_promo_codes DESC NULLS LAST;

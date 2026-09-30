WITH filtered AS (
    SELECT *, CONCAT_WS(',', promo_code, channel) AS promo_code_channel_key
    FROM promotions
    WHERE ends_on >= TO_DATE('2023-12-07')
      AND channel IS NOT DISTINCT FROM 'mobile'
),
with_windows AS (
    SELECT
        id,
        channel,
        promo_code,
        NTILE(10) OVER (PARTITION BY promo_code, channel ORDER BY discount_pct ASC, id ASC) AS discount_pct_bucket,
        NTILE(5) OVER (ORDER BY discount_pct DESC, id ASC) AS discount_pct_bucket_2,
        AVG(discount_pct) OVER (PARTITION BY channel ORDER BY starts_on ASC, id ASC ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS discount_pct_avg_5_prior,
        SUM(discount_pct) OVER (PARTITION BY channel ORDER BY starts_on ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_discount_pct
    FROM filtered
    WHERE channel IN ('mobile', 'web')
),
with_windows_promotions AS (
    SELECT
        id,
        channel,
        discount_pct_bucket,
        discount_pct_bucket_2,
        cumulative_discount_pct,
        RANK() OVER (PARTITION BY channel ORDER BY cumulative_discount_pct DESC NULLS FIRST) AS cumulative_discount_pct_rank,
        NTILE(5) OVER w_recent AS discount_pct_bucket_bucket,
        LAST_VALUE(channel) IGNORE NULLS OVER w_grp AS last_known_channel,
        ROW_NUMBER() OVER (PARTITION BY channel, promo_code ORDER BY discount_pct_bucket_2 ASC, id ASC) AS seq_no
    FROM with_windows
    WINDOW w_recent AS (PARTITION BY channel ORDER BY discount_pct_bucket DESC, id ASC), w_grp AS (ORDER BY discount_pct_bucket ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
filtered_promotions AS (
    SELECT *, CAST(discount_pct_bucket AS DOUBLE) AS discount_pct_bucket_num
    FROM with_windows_promotions
    WHERE channel IS NOT NULL
      AND discount_pct_bucket = 7
),
metrics AS (
    SELECT
        *,
        LAG(discount_pct_bucket_2) OVER (PARTITION BY channel ORDER BY discount_pct_bucket ASC, id ASC) AS prev_discount_pct_bucket_2,
        LAG(cumulative_discount_pct) OVER (PARTITION BY channel ORDER BY discount_pct_bucket ASC, id ASC) AS prev_cumulative_discount_pct,
        ROW_NUMBER() OVER (PARTITION BY channel ORDER BY discount_pct_bucket ASC, id ASC) AS seq_no_2
    FROM filtered_promotions
),
latest AS (
    SELECT
        id,
        discount_pct_bucket,
        discount_pct_bucket_2,
        cumulative_discount_pct,
        last_known_channel,
        discount_pct_bucket_num,
        prev_discount_pct_bucket_2,
        prev_cumulative_discount_pct,
        seq_no_2
    FROM metrics
    QUALIFY ROW_NUMBER() OVER (PARTITION BY channel ORDER BY cumulative_discount_pct DESC, id ASC) = 1
)
SELECT
    CASE WHEN discount_pct_bucket_2 >= 7 THEN 'HIGH' ELSE 'LOW' END AS discount_pct_bucket_2_tier,
    CASE WHEN prev_cumulative_discount_pct >= 2 THEN 'PREMIUM' ELSE 'STANDARD' END AS prev_cumulative_discount_pct_tier,
    PERCENTILE_APPROX(discount_pct_bucket_num, 0.95) AS discount_pct_bucket_num_p95,
    COUNT(DISTINCT id) AS distinct_ids,
    ROUND(AVG(discount_pct_bucket_2), 2) AS avg_discount_pct_bucket_2
FROM latest
GROUP BY CASE WHEN discount_pct_bucket_2 >= 7 THEN 'HIGH' ELSE 'LOW' END, CASE WHEN prev_cumulative_discount_pct >= 2 THEN 'PREMIUM' ELSE 'STANDARD' END
ORDER BY distinct_ids DESC NULLS LAST;

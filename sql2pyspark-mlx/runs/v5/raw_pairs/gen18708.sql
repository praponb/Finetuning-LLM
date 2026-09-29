WITH base AS (
    SELECT
        id,
        channel,
        promo_code,
        starts_on,
        ends_on,
        discount_pct,
        UPPER(channel) AS channel_upper
    FROM promotions
    WHERE ends_on <= '2024-10-05'
),
ranked AS (
    SELECT
        id,
        channel,
        promo_code,
        ends_on,
        discount_pct,
        channel_upper
    FROM base
    QUALIFY ROW_NUMBER() OVER (PARTITION BY promo_code, channel ORDER BY starts_on ASC, id ASC) = 1
),
metrics AS (
    SELECT
        id,
        discount_pct,
        SUM(discount_pct) OVER (PARTITION BY channel ORDER BY discount_pct ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_discount_pct,
        AVG(discount_pct) OVER (PARTITION BY channel, promo_code ORDER BY discount_pct ASC, id ASC ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS discount_pct_avg_2_prior,
        ROUND(discount_pct / SUM(discount_pct) OVER (PARTITION BY promo_code), 4) AS discount_pct_share
    FROM ranked
    WHERE ends_on BETWEEN '2023-12-01' AND '2025-08-01'
),
enriched AS (
    SELECT
        *,
        AVG(discount_pct_avg_2_prior) OVER w_running AS discount_pct_avg_2_prior_avg_5,
        NTILE(4) OVER w_part AS cumulative_discount_pct_bucket
    FROM metrics
    WINDOW w_running AS (ORDER BY discount_pct ASC, id ASC ROWS BETWEEN 5 PRECEDING AND CURRENT ROW), w_part AS (ORDER BY cumulative_discount_pct ASC, id ASC)
),
base_promotions AS (
    SELECT *, discount_pct * discount_pct_avg_2_prior AS discount_pct_times_discount_pct_avg_2_prior
    FROM enriched
    WHERE cumulative_discount_pct < 1
)
SELECT
    CASE WHEN cumulative_discount_pct_bucket >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END AS cumulative_discount_pct_bucket_tier,
    PERCENTILE_APPROX(cumulative_discount_pct_bucket, 0.9) AS cumulative_discount_pct_bucket_p90,
    COUNT(*) AS row_count,
    COUNT(*) AS row_count_2
FROM base_promotions
GROUP BY cumulative_discount_pct_bucket_tier
HAVING cumulative_discount_pct_bucket_p90 >= 0
ORDER BY row_count_2 ASC;

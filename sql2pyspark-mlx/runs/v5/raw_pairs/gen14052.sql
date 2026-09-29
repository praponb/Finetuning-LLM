WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        channel,
        starts_on,
        ends_on,
        TRIM(promo_code) AS clean_promo_code,
        YEAR(starts_on) AS starts_on_year
    FROM promotions
    WHERE starts_on >= DATE '2023-04-08'
      AND channel IS NOT NULL
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        channel,
        starts_on,
        ends_on,
        clean_promo_code,
        starts_on_year,
        INITCAP(TRIM(channel)) AS channel_title,
        COALESCE(channel, 'NONE') AS resolved_channel
    FROM base
    WHERE channel IS NOT NULL
      AND ends_on <= '2023-03-01'
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(starts_on_year, 1) OVER (ORDER BY starts_on, id) AS prev_starts_on_year,
        NTILE(10) OVER (ORDER BY starts_on_year DESC, id) AS starts_on_year_bucket
    FROM valid
    WHERE starts_on >= '2023-10-01'
),
metrics_promotions AS (
    -- Running and moving aggregates
    SELECT
        *,
        FIRST_VALUE(starts_on_year_bucket, TRUE) OVER (PARTITION BY channel ORDER BY ends_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_starts_on_year_bucket,
        NTILE(3) OVER (PARTITION BY channel ORDER BY starts_on_year, id) AS starts_on_year_bucket_2,
        AVG(starts_on_year_bucket) OVER (PARTITION BY channel) AS channel_avg_starts_on_year_bucket,
        LAST_VALUE(channel) IGNORE NULLS OVER (ORDER BY starts_on_year, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_channel
    FROM metrics
)
-- Grouped metrics
SELECT
    CASE WHEN starts_on_year >= 63 THEN 'HIGH' ELSE 'LOW' END AS starts_on_year_tier,
    channel,
    SUM(CASE WHEN channel = 'partner' THEN starts_on_year_bucket_2 ELSE 0 END) AS partner_starts_on_year_bucket_2,
    ROUND(SUM(prev_starts_on_year) / NULLIF(COUNT(CASE WHEN channel = 'mobile' THEN 1 END), 0), 2) AS prev_starts_on_year_per_mobile,
    APPROX_PERCENTILE(starts_on_year_bucket_2, ARRAY(0.25, 0.5, 0.75)) AS starts_on_year_bucket_2_quartiles,
    MAX(prev_starts_on_year) AS max_prev_starts_on_year,
    COUNT(CASE WHEN channel = 'mobile' THEN 1 END) AS mobile_count,
    SUM(CASE WHEN channel = 'store' THEN channel_avg_starts_on_year_bucket ELSE 0 END) AS store_channel_avg_starts_on_year_bucket
FROM metrics_promotions
GROUP BY CASE WHEN starts_on_year >= 63 THEN 'HIGH' ELSE 'LOW' END, channel
HAVING prev_starts_on_year_per_mobile >= 0;

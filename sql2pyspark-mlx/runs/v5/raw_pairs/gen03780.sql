WITH metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        LAG(discount_pct, 1) OVER (PARTITION BY channel ORDER BY starts_on, id) AS prev_discount_pct,
        AVG(discount_pct) OVER (PARTITION BY channel) AS channel_avg_discount_pct,
        NTILE(4) OVER (PARTITION BY promo_code, channel ORDER BY discount_pct, id) AS discount_pct_bucket
    FROM promotions
),
metrics_promotions AS (
    -- Running and moving aggregates
    SELECT
        id,
        promo_code,
        discount_pct,
        ROW_NUMBER() OVER w_recent AS seq_no,
        AVG(discount_pct_bucket) OVER (PARTITION BY promo_code) AS promo_code_avg_discount_pct_bucket,
        ROW_NUMBER() OVER (PARTITION BY channel, promo_code ORDER BY discount_pct DESC, id) AS seq_no_2,
        SUM(channel_avg_discount_pct) OVER (ORDER BY discount_pct_bucket, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_channel_avg_discount_pct,
        NTILE(10) OVER (PARTITION BY promo_code ORDER BY channel_avg_discount_pct, id) AS channel_avg_discount_pct_bucket
    FROM metrics
    WINDOW w_recent AS (PARTITION BY channel, promo_code ORDER BY discount_pct, id)
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        promo_code,
        promo_code_avg_discount_pct_bucket,
        cumulative_channel_avg_discount_pct,
        channel_avg_discount_pct_bucket,
        AVG(cumulative_channel_avg_discount_pct) OVER (PARTITION BY promo_code ORDER BY seq_no_2, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS cumulative_channel_avg_discount_pct_avg_3,
        NTILE(10) OVER (PARTITION BY promo_code ORDER BY promo_code_avg_discount_pct_bucket DESC, id) AS promo_code_avg_discount_pct_bucket_bucket,
        AVG(discount_pct) OVER w_recent AS discount_pct_avg_5_prior
    FROM metrics_promotions
    WINDOW w_recent AS (PARTITION BY promo_code ORDER BY seq_no_2, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING)
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT *, FLOOR(cumulative_channel_avg_discount_pct_avg_3) AS cumulative_channel_avg_discount_pct_avg_3_whole
    FROM enriched
    WHERE channel_avg_discount_pct_bucket BETWEEN 3 AND 5
),
cleaned_promotions AS (
    -- Keep recent, valid rows
    SELECT
        *,
        GREATEST(cumulative_channel_avg_discount_pct, cumulative_channel_avg_discount_pct_avg_3_whole) AS greatest_cumulative_channel_avg_discount_pct_cumulative_channel_avg_discount_pct_avg_3_whole,
        CASE WHEN promo_code = 'WELCOME5' THEN 1 ELSE 0 END AS is_welcome5
    FROM cleaned
    WHERE discount_pct_avg_5_prior = 0
)
SELECT
    id,
    promo_code,
    promo_code_avg_discount_pct_bucket,
    cumulative_channel_avg_discount_pct,
    channel_avg_discount_pct_bucket,
    cumulative_channel_avg_discount_pct_avg_3,
    promo_code_avg_discount_pct_bucket_bucket,
    discount_pct_avg_5_prior,
    greatest_cumulative_channel_avg_discount_pct_cumulative_channel_avg_discount_pct_avg_3_whole,
    CASE WHEN promo_code RLIKE '^(WELCOME5|VIP)$' THEN 'GROUP_B' WHEN promo_code RLIKE '^(SPRING10|BUNDLE)$' THEN 'PRIORITY' ELSE 'GROUP_A' END AS promo_code_zone
FROM cleaned_promotions;

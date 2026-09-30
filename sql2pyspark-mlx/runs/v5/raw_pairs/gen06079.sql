WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        LAST_DAY(ends_on) AS ends_on_month_end,
        ROUND(discount_pct * 100, 2) AS discount_pct_scaled,
        COALESCE(channel, 'N/A') AS channel_clean
    FROM promotions
    WHERE channel IS NOT NULL
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM filtered
    QUALIFY ROW_NUMBER() OVER (PARTITION BY promo_code, channel ORDER BY discount_pct DESC, id ASC) = 1
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        LENGTH(channel) AS channel_length,
        TRIM(channel) AS clean_channel
    FROM ranked
    WHERE channel IS NOT NULL
      AND starts_on >= DATE '2023-07-12'
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        promo_code,
        starts_on,
        discount_pct_scaled,
        channel_clean,
        clean_channel,
        NTILE(10) OVER (ORDER BY discount_pct_scaled ASC, id ASC) AS discount_pct_scaled_bucket,
        RANK() OVER (PARTITION BY promo_code ORDER BY discount_pct_scaled DESC NULLS FIRST) AS discount_pct_scaled_rank,
        AVG(channel_length) OVER (ORDER BY ends_on ASC, id ASC ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS channel_length_avg_2
    FROM valid
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER w_grp AS event_seq,
        NTILE(5) OVER (PARTITION BY promo_code ORDER BY discount_pct_scaled_bucket DESC, id ASC) AS discount_pct_scaled_bucket_bucket,
        DENSE_RANK() OVER (PARTITION BY promo_code ORDER BY discount_pct_scaled ASC) AS discount_pct_scaled_dense_rank
    FROM scored
    WINDOW w_grp AS (PARTITION BY promo_code ORDER BY starts_on ASC, id ASC)
)
SELECT
    id,
    discount_pct_scaled,
    event_seq,
    discount_pct_scaled_bucket_bucket
FROM enriched
ORDER BY discount_pct_scaled ASC NULLS LAST, event_seq DESC;

WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        channel,
        promo_code,
        starts_on,
        discount_pct,
        DATE_ADD(starts_on, 29) AS starts_on_plus_29d,
        COALESCE(channel, 'DIRECT') AS resolved_channel
    FROM promotions
    WHERE starts_on >= DATE '2023-05-12'
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(discount_pct / SUM(discount_pct) OVER (PARTITION BY channel), 4) AS discount_pct_share,
        LEAD(discount_pct, 1) OVER (PARTITION BY channel ORDER BY discount_pct, id) AS next_discount_pct
    FROM valid
    WHERE channel <> 'web'
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(next_discount_pct) OVER (PARTITION BY channel ORDER BY starts_on, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_avg_next_discount_pct,
        SUM(next_discount_pct) OVER (PARTITION BY channel ORDER BY starts_on, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_sum_next_discount_pct,
        MAX(next_discount_pct) OVER (PARTITION BY channel ORDER BY starts_on, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_max_next_discount_pct,
        LEAD(discount_pct, 1) OVER (PARTITION BY promo_code, channel ORDER BY discount_pct, id) AS next_discount_pct_2
    FROM scored
),
matched_keys AS (
    -- Set difference on keys
    SELECT channel
    FROM enriched
    EXCEPT DISTINCT
    SELECT channel
    FROM promotions
    WHERE discount_pct BETWEEN 0.13 AND 0.29
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM enriched s
    JOIN matched_keys k ON s.channel = k.channel
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        DATEDIFF(starts_on, starts_on_plus_29d) AS days_starts_on_plus_29d_to_starts_on,
        COALESCE(channel, 'DIRECT') AS channel_clean
    FROM kept_rows
    WHERE channel IS NOT NULL
),
scored_promotions AS (
    -- Window metrics over several different partitions
    SELECT *, STDDEV_SAMP(last3_sum_next_discount_pct) OVER (PARTITION BY channel) AS channel_stddev_samp_last3_sum_next_discount_pct
    FROM cleaned
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        channel,
        starts_on,
        starts_on_plus_29d,
        resolved_channel,
        discount_pct_share,
        next_discount_pct,
        last3_avg_next_discount_pct,
        last3_sum_next_discount_pct,
        last3_max_next_discount_pct,
        next_discount_pct_2,
        days_starts_on_plus_29d_to_starts_on,
        channel_clean,
        ROUND(next_discount_pct / discount_pct, 2) AS next_discount_pct_per_discount_pct,
        CASE WHEN discount_pct >= 0.33 THEN 'High' WHEN discount_pct >= 0.14 THEN 'Medium' ELSE 'Low' END AS discount_pct_band,
        COALESCE(channel, 'NONE') AS channel_clean_2
    FROM scored_promotions
    WHERE channel IS NOT NULL
)
SELECT
    starts_on_plus_29d,
    resolved_channel,
    discount_pct_share,
    last3_avg_next_discount_pct,
    last3_max_next_discount_pct,
    days_starts_on_plus_29d_to_starts_on,
    channel_clean,
    next_discount_pct_per_discount_pct,
    channel_clean_2
FROM filtered
ORDER BY next_discount_pct_per_discount_pct NULLS LAST, starts_on_plus_29d DESC

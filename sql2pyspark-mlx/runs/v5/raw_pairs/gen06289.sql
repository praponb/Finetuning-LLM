WITH base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        ROUND(discount_pct, 2) AS discount_pct_rounded,
        COALESCE(channel, 'OTHER') AS channel_filled
    FROM promotions
    WHERE channel IS NOT NULL
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        channel,
        starts_on,
        discount_pct,
        NTILE(5) OVER (PARTITION BY promo_code, channel ORDER BY discount_pct_rounded DESC, id) AS discount_pct_rounded_bucket,
        ROW_NUMBER() OVER (ORDER BY discount_pct, id) AS seq_no
    FROM base
    WHERE ends_on BETWEEN '2023-08-01' AND '2025-04-01'
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        seq_no * discount_pct AS seq_no_times_discount_pct,
        seq_no - discount_pct_rounded_bucket AS seq_no_minus_discount_pct_rounded_bucket
    FROM enriched
    WHERE channel IS NOT NULL
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY channel ORDER BY discount_pct_rounded_bucket DESC NULLS LAST, id) AS rn
    FROM valid
    QUALIFY rn = 1
),
deduped_promotions AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY channel ORDER BY discount_pct_rounded_bucket DESC NULLS LAST, id) AS latest_rank
    FROM deduped
    QUALIFY latest_rank = 1
)
-- Grouped metrics
SELECT
    CASE WHEN seq_no_minus_discount_pct_rounded_bucket >= 69 THEN 'LARGE' ELSE 'SMALL' END AS seq_no_minus_discount_pct_rounded_bucket_tier,
    CASE WHEN seq_no >= 8 THEN 'PREMIUM' ELSE 'STANDARD' END AS seq_no_tier,
    SIZE(COLLECT_SET(channel)) AS num_channels,
    SORT_ARRAY(COLLECT_SET(channel)) AS channel_list,
    ROUND(AVG(seq_no_times_discount_pct), 2) AS avg_seq_no_times_discount_pct,
    COUNT(*) AS row_count,
    COUNT(DISTINCT channel) AS distinct_channels,
    GROUPING_ID() AS gid
FROM deduped_promotions
GROUP BY GROUPING SETS ((CASE WHEN seq_no_minus_discount_pct_rounded_bucket >= 69 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN seq_no >= 8 THEN 'PREMIUM' ELSE 'STANDARD' END), (CASE WHEN seq_no_minus_discount_pct_rounded_bucket >= 69 THEN 'LARGE' ELSE 'SMALL' END))

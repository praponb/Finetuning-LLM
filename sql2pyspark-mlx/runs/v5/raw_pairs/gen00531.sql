WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        channel,
        promo_code,
        starts_on,
        ends_on,
        DATE_SUB(ends_on, 44) AS ends_on_minus_44d
    FROM promotions
    WHERE ends_on > DATE '2023-08-15'
      AND channel IS DISTINCT FROM 'web'
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        FIRST_VALUE(channel, TRUE) OVER w_part AS first_known_channel,
        ROW_NUMBER() OVER (PARTITION BY channel, promo_code ORDER BY starts_on ASC, id ASC) AS row_seq,
        LAST_VALUE(channel, TRUE) OVER (PARTITION BY promo_code ORDER BY ends_on ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_channel,
        FIRST_VALUE(channel, TRUE) OVER w_grp AS first_known_channel_2
    FROM filtered
    WINDOW w_part AS (PARTITION BY promo_code, channel ORDER BY ends_on_minus_44d ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_grp AS (PARTITION BY channel ORDER BY starts_on ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        channel,
        promo_code,
        starts_on,
        ends_on,
        ends_on_minus_44d,
        row_seq,
        last_known_channel,
        first_known_channel_2,
        CASE WHEN row_seq >= 8 THEN 'High' WHEN row_seq >= 3 THEN 'Medium' ELSE 'Low' END AS row_seq_band,
        CASE WHEN promo_code = 'BUNDLE' THEN 1 ELSE 0 END AS is_bundle,
        COALESCE(channel, 'NONE') AS channel_filled
    FROM with_windows
    WHERE ends_on >= DATE '2023-11-08'
      AND ends_on >= '2025-09-29'
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        channel,
        promo_code,
        row_seq,
        last_known_channel,
        row_seq_band,
        is_bundle
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY channel, promo_code ORDER BY starts_on DESC, id ASC) AS rank_in_group
        FROM cleaned
    ) x
    WHERE rank_in_group = 1
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY channel ORDER BY row_seq DESC, id ASC) AS dedup_rank
    FROM ranked
    QUALIFY dedup_rank = 1
),
ranked_promotions AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY promo_code ORDER BY row_seq DESC, id ASC) AS rank_in_group
        FROM latest
    ) ranked
    WHERE rank_in_group = 1
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(3) OVER (PARTITION BY channel ORDER BY is_bundle ASC, id ASC) AS is_bundle_bucket,
        FIRST_VALUE(is_bundle, TRUE) OVER (PARTITION BY channel ORDER BY rank_in_group ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_is_bundle
    FROM ranked_promotions
    WHERE promo_code IN ('SPRING10', 'WELCOME5', 'VIP')
)
-- Multi-level aggregation
SELECT
    CASE WHEN row_seq >= 7 THEN 'LARGE' ELSE 'SMALL' END AS row_seq_tier,
    channel,
    COUNT(*) FILTER (WHERE promo_code = 'SPRING10') AS spring10_rows,
    AVG(is_bundle_bucket) FILTER (WHERE promo_code = 'VIP') AS vip_avg_is_bundle_bucket,
    COUNT(DISTINCT id) AS distinct_ids
FROM metrics
GROUP BY CASE WHEN row_seq >= 7 THEN 'LARGE' ELSE 'SMALL' END, channel
ORDER BY spring10_rows DESC;

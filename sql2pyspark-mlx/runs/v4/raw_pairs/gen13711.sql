WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        channel,
        promo_code,
        starts_on,
        ends_on,
        discount_pct,
        DATE_SUB(starts_on, 2) AS starts_on_minus_2d,
        WEEKOFYEAR(starts_on) AS starts_on_weekofyear,
        COALESCE(channel, 'UNKNOWN') AS resolved_channel
    FROM promotions
    WHERE channel IS NOT NULL
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        channel,
        promo_code,
        starts_on,
        ends_on,
        discount_pct,
        starts_on_minus_2d,
        starts_on_weekofyear,
        resolved_channel,
        SUM(starts_on_weekofyear) OVER (PARTITION BY promo_code ORDER BY starts_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_starts_on_weekofyear,
        ROW_NUMBER() OVER (ORDER BY discount_pct DESC, id) AS row_seq,
        LAST_VALUE(starts_on_weekofyear) IGNORE NULLS OVER (ORDER BY starts_on_minus_2d, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_starts_on_weekofyear,
        RANK() OVER (PARTITION BY promo_code ORDER BY discount_pct) AS discount_pct_rank,
        CUME_DIST() OVER (ORDER BY discount_pct) AS discount_pct_cume_dist
    FROM cleaned
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        COALESCE(discount_pct_rank, 0) AS discount_pct_rank_filled,
        COALESCE(channel, 'N/A') AS resolved_channel_2
    FROM enriched
    WHERE starts_on >= DATE '2023-12-14'
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        channel,
        promo_code,
        starts_on,
        ends_on,
        discount_pct,
        starts_on_minus_2d,
        starts_on_weekofyear,
        resolved_channel,
        running_starts_on_weekofyear,
        row_seq,
        last_known_starts_on_weekofyear,
        discount_pct_rank,
        discount_pct_cume_dist,
        discount_pct_rank_filled,
        resolved_channel_2,
        PERCENT_RANK() OVER w_running AS running_starts_on_weekofyear_percent_rank,
        LAG(last_known_starts_on_weekofyear, 2, 0) OVER (PARTITION BY channel, promo_code ORDER BY row_seq, id) AS prev_last_known_starts_on_weekofyear
    FROM valid
    WINDOW w_running AS (PARTITION BY promo_code ORDER BY running_starts_on_weekofyear NULLS FIRST)
),
enriched_promotions AS (
    -- Running and moving aggregates
    SELECT
        id,
        channel,
        promo_code,
        starts_on,
        ends_on,
        discount_pct,
        starts_on_minus_2d,
        starts_on_weekofyear,
        resolved_channel,
        running_starts_on_weekofyear,
        row_seq,
        last_known_starts_on_weekofyear,
        discount_pct_rank,
        discount_pct_cume_dist,
        discount_pct_rank_filled,
        resolved_channel_2,
        running_starts_on_weekofyear_percent_rank,
        prev_last_known_starts_on_weekofyear,
        AVG(discount_pct_cume_dist) OVER (PARTITION BY promo_code ORDER BY last_known_starts_on_weekofyear, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS discount_pct_cume_dist_avg_5_prior,
        ROW_NUMBER() OVER (PARTITION BY promo_code ORDER BY discount_pct_rank_filled, id) AS event_seq,
        LAG(prev_last_known_starts_on_weekofyear, 2) OVER (PARTITION BY channel, promo_code ORDER BY discount_pct_rank_filled, id) AS prev_prev_last_known_starts_on_weekofyear,
        AVG(prev_last_known_starts_on_weekofyear) OVER (PARTITION BY channel ORDER BY ends_on, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS prev_last_known_starts_on_weekofyear_avg_6
    FROM scored
)
-- Multi-level aggregation
SELECT
    channel,
    COUNT(CASE WHEN channel = 'store' THEN 1 END) AS store_count,
    PERCENTILE_APPROX(prev_last_known_starts_on_weekofyear, 0.5) AS prev_last_known_starts_on_weekofyear_p50,
    ROUND(AVG(running_starts_on_weekofyear_percent_rank), 2) AS avg_running_starts_on_weekofyear_percent_rank
FROM enriched_promotions
GROUP BY channel
ORDER BY avg_running_starts_on_weekofyear_percent_rank

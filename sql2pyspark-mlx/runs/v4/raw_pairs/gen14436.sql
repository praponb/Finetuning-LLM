WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        channel,
        starts_on,
        ends_on,
        discount_pct,
        DATE_TRUNC('month', starts_on) starts_on_period,
        LAST_DAY(ends_on) ends_on_month_end,
        COALESCE(channel, 'OTHER') resolved_channel
    FROM promotions
    WHERE starts_on >= '2025-07-31'
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY channel ORDER BY ends_on_month_end DESC, id) AS dedup_rank
        FROM cleaned
    ) t
    WHERE dedup_rank = 1
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        MAX(discount_pct) OVER (ORDER BY CAST(starts_on_period AS BIGINT) RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW) discount_pct_max_90d,
        AVG(discount_pct) OVER (PARTITION BY channel ORDER BY ends_on, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) discount_pct_avg_2_prior,
        RANK() OVER (PARTITION BY channel ORDER BY discount_pct) discount_pct_rank,
        LEAD(discount_pct, 1) OVER (PARTITION BY channel ORDER BY discount_pct, id) next_discount_pct
    FROM deduped
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        channel,
        starts_on,
        ends_on,
        discount_pct,
        starts_on_period,
        ends_on_month_end,
        resolved_channel,
        dedup_rank,
        discount_pct_max_90d,
        discount_pct_avg_2_prior,
        discount_pct_rank,
        next_discount_pct,
        LAG(discount_pct, 1) OVER (PARTITION BY channel ORDER BY dedup_rank, id) prev_discount_pct,
        COUNT(discount_pct_rank) OVER (PARTITION BY channel ORDER BY CAST(starts_on_period AS BIGINT) RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW) discount_pct_rank_count_90d
    FROM metrics
)
-- Running and moving aggregates
SELECT
    *,
    ROUND(discount_pct_max_90d / SUM(discount_pct_max_90d) OVER (PARTITION BY channel), 4) discount_pct_max_90d_share,
    SUM(discount_pct) OVER (PARTITION BY channel) channel_sum_discount_pct,
    AVG(discount_pct_avg_2_prior) OVER (PARTITION BY channel ORDER BY discount_pct, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) discount_pct_avg_2_prior_avg_2_prior
FROM scored;

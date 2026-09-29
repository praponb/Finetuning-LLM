WITH base AS (
    -- Basic cleansing and defaults
    SELECT *, LOWER(channel) AS channel_lower
    FROM promotions
    WHERE starts_on >= TO_DATE('2023-09-25')
      AND discount_pct >= 0.19
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        channel,
        promo_code,
        starts_on,
        ends_on,
        discount_pct,
        channel_lower,
        PERCENT_RANK() OVER w_recent AS discount_pct_percent_rank,
        NTILE(5) OVER (PARTITION BY promo_code ORDER BY discount_pct, id) AS discount_pct_bucket,
        AVG(discount_pct) OVER w_recent_2 AS discount_pct_avg_6
    FROM base
    WINDOW w_recent AS (PARTITION BY promo_code ORDER BY discount_pct DESC), w_recent_2 AS (PARTITION BY promo_code ORDER BY starts_on, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        starts_on,
        discount_pct,
        discount_pct_percent_rank,
        discount_pct_bucket,
        discount_pct_avg_6,
        CAST(discount_pct AS INT) AS discount_pct_int,
        COALESCE(channel, 'OTHER') AS channel_clean
    FROM scored
    WHERE starts_on > DATE '2024-02-24'
      AND channel IS NOT NULL
      AND channel = 'partner'
),
filtered_promotions AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        ROUND(discount_pct_percent_rank, 2) AS discount_pct_percent_rank_rounded,
        LEAST(discount_pct_avg_6, discount_pct_bucket) AS least_discount_pct_avg_6_discount_pct_bucket
    FROM filtered
    WHERE starts_on > TO_DATE('2023-09-03')
      AND discount_pct <= 0.15
),
scored_promotions AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(10) OVER (ORDER BY discount_pct_percent_rank DESC, id) AS discount_pct_percent_rank_bucket,
        SUM(discount_pct_percent_rank_rounded) OVER w_hist AS running_discount_pct_percent_rank_rounded
    FROM filtered_promotions
    WINDOW w_hist AS (ORDER BY discount_pct_percent_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY running_discount_pct_percent_rank_rounded, id) AS seq_no,
        LAG(discount_pct_int, 1) OVER (ORDER BY least_discount_pct_avg_6_discount_pct_bucket, id) AS prev_discount_pct_int,
        FIRST_VALUE(discount_pct_percent_rank_bucket) IGNORE NULLS OVER (ORDER BY discount_pct_percent_rank_bucket, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_discount_pct_percent_rank_bucket,
        LEAD(running_discount_pct_percent_rank_rounded, 1) OVER (ORDER BY discount_pct_percent_rank_bucket, id) AS next_running_discount_pct_percent_rank_rounded,
        FIRST_VALUE(running_discount_pct_percent_rank_rounded) IGNORE NULLS OVER (ORDER BY starts_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_running_discount_pct_percent_rank_rounded
    FROM scored_promotions
)
-- Aggregate with conditional metrics
SELECT
    seq_no,
    ROUND(AVG(discount_pct_int), 2) AS avg_discount_pct_int,
    COUNT(DISTINCT id) AS distinct_ids,
    MIN(discount_pct_percent_rank_bucket) AS min_discount_pct_percent_rank_bucket
FROM enriched
GROUP BY 1

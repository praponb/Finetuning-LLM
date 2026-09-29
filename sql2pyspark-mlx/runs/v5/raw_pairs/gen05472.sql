WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        currency,
        EXPLODE(discounts) AS val
    FROM price_tiers
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        plan,
        effective_from,
        DATEDIFF(CURRENT_DATE(), effective_from) AS days_since_effective_from
    FROM flattened
    WHERE currency = 'USD'
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        STDDEV_SAMP(days_since_effective_from) OVER w_part AS plan_stddev_samp_days_since_effective_from,
        STDDEV_SAMP(days_since_effective_from) OVER w_part AS plan_stddev_samp_days_since_effective_from_2,
        RANK() OVER w_recent AS days_since_effective_from_rank
    FROM cleaned
    WHERE effective_from >= '2025-07-01'
    WINDOW w_part AS (PARTITION BY plan), w_recent AS (ORDER BY days_since_effective_from DESC)
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        RANK() OVER (PARTITION BY plan ORDER BY days_since_effective_from_rank) AS days_since_effective_from_rank_rank,
        DENSE_RANK() OVER (ORDER BY days_since_effective_from DESC) AS days_since_effective_from_dense_rank,
        SUM(plan_stddev_samp_days_since_effective_from_2) OVER (PARTITION BY plan) AS plan_sum_plan_stddev_samp_days_since_effective_from_2
    FROM enriched
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        effective_from,
        days_since_effective_from,
        days_since_effective_from_dense_rank,
        CUME_DIST() OVER w_ordered AS plan_sum_plan_stddev_samp_days_since_effective_from_2_cume_dist,
        PERCENT_RANK() OVER w_part AS days_since_effective_from_dense_rank_percent_rank,
        RANK() OVER (ORDER BY days_since_effective_from DESC NULLS FIRST) AS days_since_effective_from_rank_2
    FROM scored
    WINDOW w_ordered AS (PARTITION BY plan ORDER BY plan_sum_plan_stddev_samp_days_since_effective_from_2 DESC NULLS LAST), w_part AS (PARTITION BY plan ORDER BY days_since_effective_from_dense_rank NULLS FIRST)
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        effective_from,
        days_since_effective_from_dense_rank,
        plan_sum_plan_stddev_samp_days_since_effective_from_2_cume_dist,
        days_since_effective_from_dense_rank_percent_rank,
        days_since_effective_from_rank_2,
        CEIL(plan_sum_plan_stddev_samp_days_since_effective_from_2_cume_dist) AS plan_sum_plan_stddev_samp_days_since_effective_from_2_cume_dist_whole,
        ROUND(days_since_effective_from_rank_2 / days_since_effective_from_dense_rank, 2) AS days_since_effective_from_rank_2_per_days_since_effective_from_dense_rank
    FROM metrics
    WHERE effective_from >= TO_DATE('2024-05-31')
)
SELECT
    id,
    effective_from,
    days_since_effective_from_dense_rank,
    plan_sum_plan_stddev_samp_days_since_effective_from_2_cume_dist,
    plan_sum_plan_stddev_samp_days_since_effective_from_2_cume_dist_whole,
    days_since_effective_from_rank_2_per_days_since_effective_from_dense_rank
FROM base
ORDER BY days_since_effective_from_dense_rank DESC;

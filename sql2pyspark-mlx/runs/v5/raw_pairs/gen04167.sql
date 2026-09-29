WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        EXPLODE_OUTER(tier_prices) AS element
    FROM price_tiers
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, DATEDIFF(CURRENT_DATE(), effective_from) AS days_since_effective_from
    FROM flattened
    WHERE element BETWEEN 37 AND 58
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        STDDEV_SAMP(days_since_effective_from) OVER (PARTITION BY plan) AS plan_stddev_samp_days_since_effective_from,
        DENSE_RANK() OVER (PARTITION BY plan ORDER BY element NULLS FIRST) AS element_dense_rank,
        STDDEV_SAMP(element) OVER (PARTITION BY plan) AS plan_stddev_samp_element
    FROM valid
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        PERCENT_RANK() OVER (ORDER BY element_dense_rank DESC NULLS FIRST) AS element_dense_rank_percent_rank,
        DENSE_RANK() OVER (PARTITION BY plan ORDER BY element DESC NULLS FIRST) AS element_dense_rank_2
    FROM scored
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        plan,
        MAX(plan_stddev_samp_days_since_effective_from) OVER w_running AS plan_max_plan_stddev_samp_days_since_effective_from,
        CUME_DIST() OVER (PARTITION BY plan ORDER BY days_since_effective_from DESC NULLS LAST) AS days_since_effective_from_cume_dist,
        DENSE_RANK() OVER w_recent AS plan_stddev_samp_days_since_effective_from_dense_rank,
        MIN(element_dense_rank_percent_rank) OVER w_running AS plan_min_element_dense_rank_percent_rank
    FROM with_windows
    WHERE element_dense_rank_2 >= 5
    WINDOW w_running AS (PARTITION BY plan), w_recent AS (PARTITION BY plan ORDER BY plan_stddev_samp_days_since_effective_from DESC NULLS LAST)
),
valid_price_tiers AS (
    -- Basic cleansing and defaults
    SELECT
        plan,
        plan_max_plan_stddev_samp_days_since_effective_from,
        days_since_effective_from_cume_dist,
        plan_stddev_samp_days_since_effective_from_dense_rank,
        days_since_effective_from_cume_dist * plan_stddev_samp_days_since_effective_from_dense_rank AS days_since_effective_from_cume_dist_times_plan_stddev_samp_days_since_effective_from_dense_rank
    FROM metrics
    WHERE plan_max_plan_stddev_samp_days_since_effective_from IS NOT NULL
)
-- Aggregate with conditional metrics
SELECT
    plan,
    SIZE(COLLECT_SET(plan)) AS num_plans,
    PERCENTILE_APPROX(plan_max_plan_stddev_samp_days_since_effective_from, 0.95) AS plan_max_plan_stddev_samp_days_since_effective_from_p95,
    COUNT(CASE WHEN plan = 'enterprise' THEN 1 END) AS enterprise_count,
    COUNT(DISTINCT plan) AS distinct_plans
FROM valid_price_tiers
GROUP BY plan
ORDER BY enterprise_count NULLS LAST

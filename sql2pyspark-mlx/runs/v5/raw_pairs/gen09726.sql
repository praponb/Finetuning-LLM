WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        EXPLODE_OUTER(tier_prices) AS tier_value
    FROM price_tiers
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        plan,
        effective_from,
        MIN(tier_value) OVER w_running AS plan_min_tier_value,
        DENSE_RANK() OVER (ORDER BY tier_value DESC NULLS LAST) AS tier_value_dense_rank,
        SUM(tier_value) OVER w_running AS plan_sum_tier_value,
        CUME_DIST() OVER (PARTITION BY plan ORDER BY tier_value) AS tier_value_cume_dist
    FROM exploded
    WINDOW w_running AS (PARTITION BY plan)
),
kept_keys AS (
    -- Set difference on keys
    SELECT plan, id
    FROM with_windows
    EXCEPT ALL
    SELECT plan, id
    FROM price_tiers
    WHERE plan != 'basic'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_windows s
    INNER JOIN kept_keys k ON s.plan = k.plan AND s.id = k.id
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        RANK() OVER (ORDER BY plan_sum_tier_value DESC) AS plan_sum_tier_value_rank,
        CUME_DIST() OVER (PARTITION BY plan ORDER BY tier_value_cume_dist DESC) AS tier_value_cume_dist_cume_dist,
        SUM(tier_value_cume_dist) OVER (PARTITION BY plan) AS plan_sum_tier_value_cume_dist,
        STDDEV_SAMP(tier_value_cume_dist) OVER (PARTITION BY plan) AS plan_stddev_samp_tier_value_cume_dist
    FROM kept_rows
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        plan,
        effective_from,
        plan_min_tier_value,
        tier_value_dense_rank,
        plan_sum_tier_value,
        tier_value_cume_dist,
        tier_value_cume_dist_cume_dist,
        plan_sum_tier_value_cume_dist,
        plan_stddev_samp_tier_value_cume_dist,
        CAST(tier_value_dense_rank AS DOUBLE) AS tier_value_dense_rank_num
    FROM metrics
    WHERE effective_from >= DATE '2024-02-02'
)
-- Grouped metrics
SELECT
    plan,
    DATE_TRUNC('WEEK', effective_from) AS week_start,
    ROUND(AVG(plan_stddev_samp_tier_value_cume_dist), 2) AS avg_plan_stddev_samp_tier_value_cume_dist,
    COUNT(DISTINCT plan) AS distinct_plans,
    SIZE(COLLECT_SET(plan)) AS num_plans,
    SUM(tier_value_cume_dist) FILTER (WHERE plan = 'enterprise') AS enterprise_total_tier_value_cume_dist,
    PERCENTILE_APPROX(plan_min_tier_value, 0.95) AS plan_min_tier_value_p95
FROM valid
GROUP BY plan, DATE_TRUNC('WEEK', effective_from);

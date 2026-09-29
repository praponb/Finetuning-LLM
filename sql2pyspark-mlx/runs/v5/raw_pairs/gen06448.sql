WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        EXPLODE_OUTER(discounts) AS tier_value
    FROM price_tiers
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        plan,
        tier_value,
        AVG(tier_value) OVER w_recent AS plan_avg_tier_value,
        RANK() OVER (ORDER BY tier_value DESC) AS tier_value_rank,
        CUME_DIST() OVER w_recent_2 AS tier_value_cume_dist,
        MAX(tier_value) OVER w_recent AS plan_max_tier_value,
        CUME_DIST() OVER (PARTITION BY plan ORDER BY tier_value DESC NULLS FIRST) AS tier_value_cume_dist_2
    FROM exploded
    WINDOW w_recent AS (PARTITION BY plan), w_recent_2 AS (PARTITION BY plan ORDER BY tier_value DESC NULLS LAST)
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, plan_avg_tier_value + tier_value_cume_dist AS plan_avg_tier_value_plus_tier_value_cume_dist
    FROM scored
    WHERE plan <> 'pro'
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        plan,
        tier_value,
        plan_max_tier_value,
        tier_value_cume_dist_2,
        plan_avg_tier_value_plus_tier_value_cume_dist,
        tier_value_cume_dist * plan_avg_tier_value AS tier_value_cume_dist_times_plan_avg_tier_value
    FROM base
    WHERE tier_value BETWEEN 30 AND 54
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        MIN(plan_max_tier_value) OVER w_running AS plan_min_plan_max_tier_value,
        CUME_DIST() OVER (ORDER BY plan_max_tier_value NULLS LAST) AS plan_max_tier_value_cume_dist,
        PERCENT_RANK() OVER (PARTITION BY plan ORDER BY plan_avg_tier_value_plus_tier_value_cume_dist NULLS FIRST) AS plan_avg_tier_value_plus_tier_value_cume_dist_percent_rank,
        MIN(tier_value_cume_dist_times_plan_avg_tier_value) OVER w_running AS plan_min_tier_value_cume_dist_times_plan_avg_tier_value,
        MIN(tier_value_cume_dist_2) OVER w_running AS plan_min_tier_value_cume_dist_2
    FROM filtered
    WINDOW w_running AS (PARTITION BY plan)
)
SELECT
    id,
    plan,
    tier_value,
    plan_max_tier_value,
    plan_min_plan_max_tier_value,
    plan_max_tier_value_cume_dist,
    plan_avg_tier_value_plus_tier_value_cume_dist_percent_rank,
    plan_min_tier_value_cume_dist_times_plan_avg_tier_value
FROM metrics
ORDER BY plan_max_tier_value DESC;

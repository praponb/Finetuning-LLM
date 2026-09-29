WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        EXPLODE(discounts) AS tier_value
    FROM price_tiers
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CAST(tier_value AS INT) AS tier_value_int,
        UPPER(plan) AS plan_upper
    FROM exploded
    WHERE plan IN ('starter', 'enterprise', 'basic')
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        plan,
        tier_value,
        tier_value_int,
        plan_upper,
        AVG(tier_value) OVER (PARTITION BY plan) AS plan_avg_tier_value,
        AVG(tier_value) OVER (PARTITION BY plan) AS plan_avg_tier_value_2,
        SUM(tier_value_int) OVER (PARTITION BY plan) AS plan_sum_tier_value_int,
        PERCENT_RANK() OVER (PARTITION BY plan ORDER BY tier_value DESC NULLS FIRST) AS tier_value_percent_rank
    FROM base
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        PERCENT_RANK() OVER (PARTITION BY plan ORDER BY plan_avg_tier_value_2 DESC NULLS FIRST) AS plan_avg_tier_value_2_percent_rank,
        MAX(tier_value_int) OVER w_hist AS plan_max_tier_value_int,
        MAX(tier_value) OVER w_hist AS plan_max_tier_value
    FROM metrics
    WINDOW w_hist AS (PARTITION BY plan)
)
SELECT
    id,
    tier_value,
    tier_value_int,
    plan_upper,
    plan_avg_tier_value,
    plan_avg_tier_value_2,
    tier_value_percent_rank,
    plan_avg_tier_value_2_percent_rank,
    plan_max_tier_value
FROM scored
ORDER BY plan_max_tier_value NULLS LAST, plan_avg_tier_value DESC

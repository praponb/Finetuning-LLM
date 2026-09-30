WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        EXPLODE_OUTER(tier_limits) AS tier_value
    FROM price_tiers
),
filtered AS (
    -- Keep recent, valid rows
    SELECT *, CAST(tier_value AS DOUBLE) AS tier_value_str
    FROM exploded
    WHERE effective_from > DATE '2024-03-06'
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        plan,
        effective_from,
        tier_value,
        tier_value_str,
        DENSE_RANK() OVER (ORDER BY tier_value_str DESC) AS tier_value_str_dense_rank,
        AVG(tier_value_str) OVER (PARTITION BY plan) AS plan_avg_tier_value_str,
        DENSE_RANK() OVER (ORDER BY tier_value_str DESC NULLS FIRST) AS tier_value_str_dense_rank_2
    FROM filtered
),
with_windows AS (
    -- Ranking and gap features
    SELECT *, SUM(plan_avg_tier_value_str) OVER (PARTITION BY plan) AS plan_sum_plan_avg_tier_value_str
    FROM metrics
),
metrics_price_tiers AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        MAX(tier_value) OVER (PARTITION BY plan) AS plan_max_tier_value,
        SUM(plan_sum_plan_avg_tier_value_str) OVER (PARTITION BY plan) AS plan_sum_plan_sum_plan_avg_tier_value_str,
        PERCENT_RANK() OVER (PARTITION BY plan ORDER BY plan_sum_plan_avg_tier_value_str ASC NULLS LAST) AS plan_sum_plan_avg_tier_value_str_percent_rank
    FROM with_windows
    WHERE plan_avg_tier_value_str < 26
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT id
    FROM metrics_price_tiers
    INTERSECT
    SELECT id
    FROM price_tiers
    WHERE currency <> 'EUR'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM metrics_price_tiers s
    JOIN remaining_keys k ON s.id = k.id
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        plan,
        plan_sum_plan_avg_tier_value_str_percent_rank,
        tier_value_str
    FROM verified
) src
UNPIVOT (
    value FOR measure IN (plan_sum_plan_avg_tier_value_str_percent_rank AS PLAN_SUM_PLAN_AVG_TIER_VALUE_STR_PERCENT_RANK_VAL, tier_value_str AS TIER_VALUE_STR_VAL)
)

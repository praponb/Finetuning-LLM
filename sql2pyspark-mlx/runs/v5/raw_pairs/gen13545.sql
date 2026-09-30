WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        tier_value
    FROM price_tiers
    LATERAL VIEW EXPLODE(discounts) li AS tier_value
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        plan,
        AVG(tier_value) OVER (PARTITION BY plan) AS plan_avg_tier_value
    FROM lines
),
remaining_keys AS (
    -- Set difference on keys
    SELECT plan, id
    FROM with_windows
    EXCEPT
    SELECT plan, id
    FROM price_tiers
    WHERE plan NOT IN ('enterprise', 'basic')
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_windows s
    JOIN remaining_keys k ON s.plan = k.plan AND s.id = k.id
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        STDDEV_SAMP(plan_avg_tier_value) OVER (PARTITION BY plan) AS plan_stddev_samp_plan_avg_tier_value,
        AVG(plan_avg_tier_value) OVER (PARTITION BY plan) AS plan_avg_plan_avg_tier_value
    FROM kept_rows
),
filtered AS (
    -- Keep recent, valid rows
    SELECT *, FLOOR(plan_avg_plan_avg_tier_value) AS plan_avg_plan_avg_tier_value_whole
    FROM scored
    WHERE plan_stddev_samp_plan_avg_tier_value BETWEEN 34 AND 64
),
matched_keys AS (
    -- Set intersection on keys
    SELECT plan, id
    FROM filtered
    INTERSECT
    SELECT plan, id
    FROM price_tiers
    WHERE YEAR(effective_from) = 2025
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM filtered s
    JOIN matched_keys k ON s.plan = k.plan AND s.id = k.id
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        plan_avg_tier_value,
        plan_stddev_samp_plan_avg_tier_value,
        plan_avg_plan_avg_tier_value,
        plan_avg_plan_avg_tier_value_whole,
        RANK() OVER (ORDER BY plan_avg_plan_avg_tier_value) AS plan_avg_plan_avg_tier_value_rank,
        DENSE_RANK() OVER (PARTITION BY plan ORDER BY plan_avg_tier_value DESC NULLS FIRST) AS plan_avg_tier_value_dense_rank,
        MIN(plan_avg_tier_value) OVER (PARTITION BY plan) AS plan_min_plan_avg_tier_value,
        CUME_DIST() OVER (ORDER BY plan_avg_plan_avg_tier_value_whole NULLS LAST) AS plan_avg_plan_avg_tier_value_whole_cume_dist
    FROM verified
    WHERE plan_avg_plan_avg_tier_value > 53
)
SELECT
    plan_avg_tier_value,
    plan_avg_plan_avg_tier_value,
    plan_avg_plan_avg_tier_value_whole,
    plan_avg_tier_value_dense_rank,
    plan_min_plan_avg_tier_value,
    plan_avg_plan_avg_tier_value_whole_cume_dist
FROM enriched
ORDER BY plan_avg_tier_value NULLS LAST;

WITH lines AS (
    SELECT
        id,
        plan,
        tier_value
    FROM price_tiers
    LATERAL VIEW EXPLODE(tier_limits) li AS tier_value
),
enriched AS (
    SELECT
        id,
        plan,
        tier_value,
        DENSE_RANK() OVER (ORDER BY tier_value ASC NULLS FIRST) tier_value_dense_rank,
        DENSE_RANK() OVER (PARTITION BY plan ORDER BY tier_value DESC NULLS FIRST) tier_value_dense_rank_2,
        COUNT(tier_value) OVER (PARTITION BY plan) plan_count_tier_value,
        AVG(tier_value) OVER (PARTITION BY plan) plan_avg_tier_value,
        RANK() OVER (ORDER BY tier_value DESC NULLS LAST) tier_value_rank
    FROM lines
),
kept_keys AS (
    SELECT plan
    FROM enriched
    INTERSECT ALL
    SELECT plan
    FROM price_tiers
    WHERE currency = 'THB'
),
reconciled AS (
    SELECT s.*
    FROM enriched s
    INNER JOIN kept_keys k ON s.plan = k.plan
),
scored AS (
    SELECT
        plan,
        tier_value,
        tier_value_dense_rank,
        tier_value_dense_rank_2,
        plan_count_tier_value,
        plan_avg_tier_value,
        tier_value_rank,
        MAX(tier_value_rank) OVER w_ordered plan_max_tier_value_rank,
        MIN(tier_value_rank) OVER w_ordered plan_min_tier_value_rank
    FROM reconciled
    WINDOW w_ordered AS (PARTITION BY plan)
),
valid AS (
    SELECT *, ROUND(tier_value / plan_count_tier_value, 2) tier_value_per_plan_count_tier_value
    FROM scored
    WHERE tier_value < 59
)
SELECT
    plan,
    tier_value,
    tier_value_dense_rank,
    tier_value_dense_rank_2,
    plan_count_tier_value,
    plan_avg_tier_value,
    tier_value_rank,
    tier_value_per_plan_count_tier_value
FROM valid
ORDER BY tier_value_rank DESC NULLS LAST

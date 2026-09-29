WITH lines AS (
    SELECT
        id,
        plan,
        EXPLODE(tier_limits) AS tier_value
    FROM price_tiers
)
SELECT
    id,
    plan,
    SUM(tier_value) OVER w_ordered AS plan_sum_tier_value,
    MIN(tier_value) OVER w_ordered AS plan_min_tier_value
FROM lines
WHERE plan = 'starter'
WINDOW w_ordered AS (PARTITION BY plan)

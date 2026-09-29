WITH exploded AS (
    SELECT
        id,
        plan,
        EXPLODE_OUTER(tier_limits) AS tier_value
    FROM price_tiers
)
SELECT
    id,
    tier_value,
    PERCENT_RANK() OVER (ORDER BY tier_value DESC NULLS FIRST) AS tier_value_percent_rank,
    MAX(tier_value) OVER w_running AS plan_max_tier_value,
    COUNT(tier_value) OVER w_running AS plan_count_tier_value
FROM exploded
WINDOW w_running AS (PARTITION BY plan)

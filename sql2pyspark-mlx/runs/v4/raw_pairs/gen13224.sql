WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        EXPLODE(discounts) AS tier_value
    FROM price_tiers
)
-- Running and moving aggregates
SELECT
    id,
    plan,
    tier_value,
    PERCENT_RANK() OVER (PARTITION BY plan ORDER BY tier_value) AS tier_value_percent_rank,
    MIN(tier_value) OVER w_ordered AS plan_min_tier_value,
    DENSE_RANK() OVER w_recent AS tier_value_dense_rank
FROM exploded
WINDOW w_ordered AS (PARTITION BY plan), w_recent AS (PARTITION BY plan ORDER BY tier_value DESC NULLS FIRST);

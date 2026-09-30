WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        currency,
        tier_value
    FROM price_tiers
    LATERAL VIEW EXPLODE(tier_limits) e AS tier_value
)
-- Running and moving aggregates
SELECT
    id,
    currency,
    DENSE_RANK() OVER (PARTITION BY plan ORDER BY tier_value DESC) AS tier_value_dense_rank,
    MAX(tier_value) OVER (PARTITION BY plan) AS plan_max_tier_value
FROM flattened
WHERE effective_from > '2024-05-16';

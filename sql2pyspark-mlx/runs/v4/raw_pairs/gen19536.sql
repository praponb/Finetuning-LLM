WITH exploded AS (
    SELECT
        id,
        plan,
        effective_from,
        currency,
        tier_value
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(discounts) li AS tier_value
)
SELECT
    id,
    plan,
    effective_from,
    currency,
    tier_value,
    PERCENT_RANK() OVER (ORDER BY tier_value DESC NULLS FIRST) AS tier_value_percent_rank,
    DENSE_RANK() OVER (ORDER BY tier_value) AS tier_value_dense_rank,
    MIN(tier_value) OVER (PARTITION BY plan) AS plan_min_tier_value
FROM exploded;

WITH flattened AS (
    SELECT
        id,
        plan,
        effective_from,
        currency,
        EXPLODE(tier_limits) AS tier_value
    FROM price_tiers
)
SELECT
    currency,
    plan,
    ROUND(SUM(tier_value) / NULLIF(COUNT(CASE WHEN currency = 'THB' THEN 1 END), 0), 2) AS tier_value_per_thb,
    PERCENTILE_APPROX(tier_value, 0.5) AS tier_value_p50,
    COUNT(DISTINCT plan) AS distinct_plans
FROM flattened
GROUP BY GROUPING SETS ((currency, plan), (currency), ());

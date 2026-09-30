WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        tier_value
    FROM price_tiers
    LATERAL VIEW EXPLODE(discounts) e AS tier_value
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('QUARTER', effective_from) AS quarter_start,
    COUNT(*) AS num_rows,
    SUM(CASE WHEN plan = 'pro' THEN tier_value ELSE 0 END) AS pro_tier_value,
    PERCENTILE_APPROX(tier_value, ARRAY(0.25, 0.5, 0.75)) AS tier_value_quartiles,
    COUNT(*) FILTER (WHERE plan = 'basic') AS basic_rows,
    COUNT_IF(plan = 'enterprise') AS enterprise_events,
    COUNT(DISTINCT plan) AS distinct_plans
FROM exploded
GROUP BY DATE_TRUNC('QUARTER', effective_from)
ORDER BY enterprise_events ASC NULLS FIRST

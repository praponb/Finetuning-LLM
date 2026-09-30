WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        EXPLODE_OUTER(tier_limits) AS tier_value
    FROM price_tiers
)
-- Aggregate with conditional metrics
SELECT
    plan,
    COUNT(*) FILTER (WHERE plan = 'pro') AS pro_rows,
    COUNT_IF(plan = 'pro') AS pro_events,
    ROUND(AVG(tier_value), 2) AS avg_tier_value,
    SUM(tier_value) FILTER (WHERE plan = 'enterprise') AS enterprise_total_tier_value,
    COUNT(CASE WHEN plan = 'basic' THEN 1 END) AS basic_count,
    APPROX_PERCENTILE(tier_value, ARRAY(0.25, 0.5, 0.75)) AS tier_value_quartiles
FROM lines
GROUP BY plan
HAVING COUNT(*) FILTER (WHERE plan = 'pro') >= 1
ORDER BY pro_rows DESC NULLS FIRST;

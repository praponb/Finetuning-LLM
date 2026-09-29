WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        currency,
        EXPLODE(tier_limits) AS tier_value
    FROM price_tiers
)
-- Grouped metrics
SELECT
    DATE_TRUNC('QUARTER', effective_from) AS quarter_start,
    CASE WHEN tier_value >= 67 THEN 'PREMIUM' ELSE 'STANDARD' END AS tier_value_tier,
    ROUND(SUM(tier_value) / NULLIF(COUNT(CASE WHEN currency = 'USD' THEN 1 END), 0), 2) AS tier_value_per_usd,
    SUM(tier_value) AS total_tier_value,
    COUNT_IF(plan = 'enterprise') AS enterprise_events
FROM exploded
GROUP BY 1, 2;

WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        EXPLODE_OUTER(tier_limits) AS tier_value
    FROM price_tiers
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT *, ROUND(tier_value, 1) AS tier_value_rounded
    FROM exploded
    WHERE tier_value <= 62
),
filtered_price_tiers AS (
    -- Basic cleansing and defaults
    SELECT *, CASE WHEN plan IN ('starter', 'enterprise') THEN 'Group A' ELSE 'Other' END AS plan_group
    FROM filtered
    WHERE tier_value_rounded >= 76
),
matched_keys AS (
    -- Set intersection on keys
    SELECT id, plan
    FROM filtered_price_tiers
    INTERSECT ALL
    SELECT id, plan
    FROM price_tiers
    WHERE effective_from > '2023-07-01'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM filtered_price_tiers s
    INNER JOIN matched_keys k ON s.id = k.id AND s.plan = k.plan
)
-- Grouped metrics
SELECT
    plan,
    APPROX_PERCENTILE(tier_value, ARRAY(0.25, 0.5, 0.75)) AS tier_value_quartiles,
    ROUND(SUM(tier_value_rounded) / NULLIF(COUNT(CASE WHEN plan = 'starter' THEN 1 END), 0), 2) AS tier_value_rounded_per_starter,
    APPROX_PERCENTILE(tier_value, ARRAY(0.25, 0.5, 0.75)) AS tier_value_quartiles_2,
    COUNT(*) FILTER (WHERE plan = 'enterprise') AS enterprise_rows
FROM verified
GROUP BY 1
ORDER BY enterprise_rows NULLS FIRST

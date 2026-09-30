WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        EXPLODE(discounts) AS val
    FROM price_tiers
)
-- Multi-level aggregation
SELECT
    plan,
    CASE WHEN val >= 81 THEN 'LARGE' ELSE 'SMALL' END AS val_tier,
    APPROX_PERCENTILE(val, ARRAY(0.25, 0.5, 0.75)) AS val_quartiles,
    ROUND(SUM(val) / NULLIF(COUNT(CASE WHEN plan = 'starter' THEN 1 END), 0), 2) AS val_per_starter,
    MAX(val) AS max_val
FROM exploded
GROUP BY GROUPING SETS ((plan, CASE WHEN val >= 81 THEN 'LARGE' ELSE 'SMALL' END), (plan))
ORDER BY val_per_starter DESC NULLS LAST

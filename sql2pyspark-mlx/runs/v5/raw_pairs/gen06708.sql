WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT id, tier_value
    FROM price_tiers
    LATERAL VIEW EXPLODE(discounts) x AS tier_value
),
matched_keys AS (
    -- Set difference on keys
    SELECT id
    FROM flattened
    EXCEPT
    SELECT id
    FROM price_tiers
    WHERE MONTH(effective_from) IN (2, 3, 9)
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM flattened s
    JOIN matched_keys k ON s.id = k.id
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CEIL(tier_value) AS tier_value_whole
    FROM verified
    WHERE tier_value BETWEEN 45 AND 60
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, ROUND(tier_value * 100, 2) AS adjusted_tier_value
    FROM filtered
    WHERE tier_value >= 80
)
-- Multi-level aggregation
SELECT
    CASE WHEN adjusted_tier_value >= 89 THEN 'HIGH' ELSE 'LOW' END AS adjusted_tier_value_tier,
    PERCENTILE_APPROX(tier_value_whole, ARRAY(0.25, 0.5, 0.75)) AS tier_value_whole_quartiles,
    SUM(adjusted_tier_value) AS total_adjusted_tier_value,
    APPROX_PERCENTILE(tier_value_whole, 0.95) AS tier_value_whole_p95,
    COUNT(DISTINCT id) AS distinct_ids
FROM valid
GROUP BY CASE WHEN adjusted_tier_value >= 89 THEN 'HIGH' ELSE 'LOW' END
ORDER BY tier_value_whole_p95 ASC;

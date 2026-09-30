WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT id, EXPLODE_OUTER(tier_limits) AS tier_value
    FROM price_tiers
),
base AS (
    -- Basic cleansing and defaults
    SELECT *, CEIL(tier_value) AS tier_value_whole
    FROM exploded
    WHERE tier_value > 67
),
matched_keys AS (
    -- Set intersection on keys
    SELECT id
    FROM base
    INTERSECT ALL
    SELECT id
    FROM price_tiers
    WHERE MONTH(effective_from) IN (7, 8, 12)
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM base s
    JOIN matched_keys k ON s.id = k.id
),
filtered AS (
    -- Keep recent, valid rows
    SELECT *, tier_value + tier_value_whole AS tier_value_plus_tier_value_whole
    FROM kept_rows
    WHERE tier_value = 71
)
-- Grouped metrics
SELECT
    CASE WHEN tier_value_whole >= 85 THEN 'PREMIUM' ELSE 'STANDARD' END AS tier_value_whole_tier,
    ROUND(AVG(tier_value_whole), 2) AS avg_tier_value_whole,
    APPROX_PERCENTILE(tier_value_whole, ARRAY(0.25, 0.5, 0.75)) AS tier_value_whole_quartiles,
    PERCENTILE_APPROX(tier_value, ARRAY(0.25, 0.5, 0.75)) AS tier_value_quartiles
FROM filtered
GROUP BY tier_value_whole_tier;

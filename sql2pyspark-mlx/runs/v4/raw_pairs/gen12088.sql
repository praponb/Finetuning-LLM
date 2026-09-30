WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        currency,
        tier_value
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(tier_prices) t AS tier_value
),
base AS (
    -- Keep recent, valid rows
    SELECT *, CAST(tier_value AS INT) AS tier_value_int
    FROM lines
    WHERE tier_value >= 78
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT *, tier_value / NULLIF(tier_value_int, 0) AS tier_value_per_tier_value_int
    FROM base
    WHERE currency NOT IN ('EUR', 'THB')
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        currency,
        tier_value,
        tier_value_per_tier_value_int
    FROM filtered
) src
UNPIVOT INCLUDE NULLS (
    metric_value FOR metric_name IN (tier_value AS TIER_VALUE_VAL, tier_value_per_tier_value_int AS TIER_VALUE_PER_TIER_VALUE_INT)
);

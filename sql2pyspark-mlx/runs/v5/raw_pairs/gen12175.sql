WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        currency,
        EXPLODE_OUTER(tier_prices) AS val
    FROM price_tiers
),
base AS (
    -- Keep recent, valid rows
    SELECT
        plan,
        currency,
        DATEDIFF(CURRENT_DATE(), effective_from) AS days_since_effective_from,
        COALESCE(val, 0) AS val_filled
    FROM flattened
    WHERE effective_from >= TO_DATE('2024-03-23')
),
kept_keys AS (
    -- Set intersection on keys
    SELECT plan, currency
    FROM base
    INTERSECT ALL
    SELECT plan, currency
    FROM price_tiers
    WHERE effective_from >= '2024-08-01'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM base s
    JOIN kept_keys k ON s.plan = k.plan AND s.currency = k.currency
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT *, ROUND(days_since_effective_from * 1.07, 2) AS days_since_effective_from_adjusted
    FROM kept_rows
    WHERE val_filled > 29
)
-- Grouped metrics
SELECT
    currency,
    CASE WHEN days_since_effective_from >= 85 THEN 'HIGH' ELSE 'LOW' END AS days_since_effective_from_tier,
    CASE WHEN val_filled >= 66 THEN 'HIGH' ELSE 'LOW' END AS val_filled_tier,
    PERCENTILE_APPROX(days_since_effective_from, 0.5) AS days_since_effective_from_p50,
    PERCENTILE_APPROX(days_since_effective_from, ARRAY(0.25, 0.5, 0.75)) AS days_since_effective_from_quartiles,
    PERCENTILE_APPROX(val_filled, 0.95) AS val_filled_p95
FROM filtered
GROUP BY currency, days_since_effective_from_tier, val_filled_tier
ORDER BY val_filled_p95 NULLS LAST;

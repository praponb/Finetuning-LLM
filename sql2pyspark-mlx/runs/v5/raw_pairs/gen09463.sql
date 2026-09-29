WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        EXPLODE_OUTER(tier_prices) AS val
    FROM price_tiers
),
valid AS (
    -- Basic cleansing and defaults
    SELECT *, UPPER(plan) AS plan_upper
    FROM lines
    WHERE plan IN ('enterprise', 'pro', 'basic')
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, COALESCE(val, 0) AS val_filled
    FROM valid
    WHERE val IS NOT NULL
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        plan,
        plan_upper,
        val_filled,
        PERCENT_RANK() OVER (PARTITION BY plan ORDER BY val NULLS FIRST) AS val_percent_rank
    FROM filtered
    WHERE val > 60
)
-- Multi-level aggregation
SELECT
    plan,
    SUM(CASE WHEN plan = 'basic' THEN val_percent_rank ELSE 0 END) AS basic_val_percent_rank,
    ROUND(AVG(val_percent_rank), 2) AS avg_val_percent_rank,
    COUNT(CASE WHEN plan = 'enterprise' THEN 1 END) AS enterprise_count,
    ROUND(SUM(val_filled) / NULLIF(COUNT(CASE WHEN plan = 'pro' THEN 1 END), 0), 2) AS val_filled_per_pro,
    ROUND(AVG(val_filled), 2) AS avg_val_filled,
    SORT_ARRAY(COLLECT_SET(plan)) AS plan_list
FROM enriched
GROUP BY plan
HAVING avg_val_filled >= 0

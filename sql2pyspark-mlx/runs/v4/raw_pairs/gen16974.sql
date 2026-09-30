WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        EXPLODE(tier_prices) AS val
    FROM price_tiers
),
valid AS (
    -- Basic cleansing and defaults
    SELECT *, ROUND(val * 1.1, 2) AS adjusted_val
    FROM lines
    WHERE plan != 'basic'
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, GREATEST(adjusted_val, val) AS greatest_adjusted_val_val
    FROM valid
    WHERE val BETWEEN 49 AND 80
),
valid_price_tiers AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        plan,
        val,
        CAST(adjusted_val AS INT) AS adjusted_val_int
    FROM filtered
    WHERE val IS NOT NULL
),
filtered_price_tiers AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        GREATEST(val, adjusted_val_int) AS greatest_val_adjusted_val_int,
        LENGTH(plan) AS plan_length
    FROM valid_price_tiers
    WHERE adjusted_val_int IS NOT NULL
),
kept_keys AS (
    -- Set intersection on keys
    SELECT id
    FROM filtered_price_tiers
    INTERSECT
    SELECT id
    FROM price_tiers
    WHERE MONTH(effective_from) IN (3, 9, 10)
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM filtered_price_tiers s
    JOIN kept_keys k ON s.id = k.id
),
totals AS (
    -- Grouped metrics
    SELECT
        CASE WHEN plan_length >= 69 THEN 'LARGE' ELSE 'SMALL' END AS plan_length_tier,
        plan,
        COUNT(*) FILTER (WHERE plan = 'starter') AS starter_rows,
        SUM(val) FILTER (WHERE plan = 'starter') AS starter_total_val,
        SUM(adjusted_val_int) AS total_adjusted_val_int,
        SORT_ARRAY(COLLECT_SET(plan)) AS plan_list,
        SIZE(COLLECT_SET(plan)) AS num_plans,
        COUNT(CASE WHEN plan = 'starter' THEN 1 END) AS starter_count,
        PERCENT_RANK() OVER (PARTITION BY CASE WHEN plan_length >= 69 THEN 'LARGE' ELSE 'SMALL' END ORDER BY SIZE(COLLECT_SET(plan)) DESC) AS num_plans_percent_rank
    FROM kept_rows
    GROUP BY CASE WHEN plan_length >= 69 THEN 'LARGE' ELSE 'SMALL' END, plan
)
SELECT
    plan_length_tier,
    plan,
    starter_total_val,
    total_adjusted_val_int,
    plan_list,
    num_plans,
    num_plans_percent_rank
FROM totals
ORDER BY plan_length_tier NULLS LAST;

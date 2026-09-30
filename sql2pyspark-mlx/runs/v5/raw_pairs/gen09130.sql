WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        val
    FROM price_tiers
    LATERAL VIEW EXPLODE(discounts) e AS val
),
valid AS (
    -- Keep recent, valid rows
    SELECT *, LAST_DAY(effective_from) AS effective_from_month_end
    FROM flattened
    WHERE val > 47
),
kept_keys AS (
    -- Set intersection on keys
    SELECT id
    FROM valid
    INTERSECT ALL
    SELECT id
    FROM price_tiers
    WHERE effective_from BETWEEN '2023-06-01' AND '2024-03-01'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM valid s
    INNER JOIN kept_keys k ON s.id = k.id
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, SUBSTRING(plan, 1, 2) AS plan_prefix
    FROM verified
    WHERE effective_from > DATE '2024-06-16'
),
stats AS (
    -- Grouped metrics
    SELECT
        CASE WHEN val >= 87 THEN 'HIGH' ELSE 'LOW' END AS val_tier,
        plan,
        COUNT(DISTINCT id) AS distinct_ids,
        APPROX_PERCENTILE(val, 0.5) AS val_p50,
        SUM(val) FILTER (WHERE plan = 'pro') AS pro_total_val
    FROM cleaned
    GROUP BY CASE WHEN val >= 87 THEN 'HIGH' ELSE 'LOW' END, plan
)
SELECT
    val_tier,
    plan,
    distinct_ids,
    val_p50
FROM stats;

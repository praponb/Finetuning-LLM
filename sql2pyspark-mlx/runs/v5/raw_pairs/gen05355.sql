WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        val
    FROM price_tiers
    LATERAL VIEW EXPLODE(discounts) e AS val
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        LOWER(plan) AS plan_lower,
        COALESCE(val, 0) AS val_filled
    FROM lines
    WHERE effective_from >= DATE '2024-03-25'
),
kept_keys AS (
    -- Set intersection on keys
    SELECT id, plan
    FROM filtered
    INTERSECT
    SELECT id, plan
    FROM price_tiers
    WHERE currency = 'THB'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM filtered s
    INNER JOIN kept_keys k ON s.id = k.id AND s.plan = k.plan
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        val,
        val_filled,
        MAX(val_filled) OVER (PARTITION BY plan) AS plan_max_val_filled,
        PERCENT_RANK() OVER (PARTITION BY plan ORDER BY val_filled) AS val_filled_percent_rank,
        CUME_DIST() OVER (ORDER BY val DESC NULLS LAST) AS val_cume_dist
    FROM reconciled
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        val_filled,
        plan_max_val_filled,
        val_filled_percent_rank,
        CASE WHEN plan_max_val_filled >= 73 THEN 'High' WHEN plan_max_val_filled >= 22 THEN 'Medium' ELSE 'Low' END AS plan_max_val_filled_band,
        FLOOR(val) AS val_whole
    FROM enriched
    WHERE val_cume_dist <= 0.47
)
SELECT
    id,
    plan_max_val_filled,
    val_filled_percent_rank,
    val_whole
FROM valid
ORDER BY val_whole DESC NULLS LAST, val_filled_percent_rank;

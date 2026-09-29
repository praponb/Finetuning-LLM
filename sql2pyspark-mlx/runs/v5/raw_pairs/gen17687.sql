WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        currency,
        EXPLODE(discounts) AS val
    FROM price_tiers
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(val) OVER (PARTITION BY currency, plan) AS currency_avg_val,
        CUME_DIST() OVER (PARTITION BY plan ORDER BY val DESC NULLS LAST) AS val_cume_dist,
        COUNT(val) OVER (PARTITION BY plan) AS plan_count_val
    FROM exploded
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT *, SUBSTRING(plan, 1, 2) AS plan_prefix
    FROM enriched
    WHERE effective_from > DATE '2024-05-01'
),
cleaned_price_tiers AS (
    -- Keep recent, valid rows
    SELECT
        id,
        plan,
        currency,
        val_cume_dist,
        plan_count_val,
        plan_prefix,
        ROUND(val_cume_dist * 1.07, 2) AS val_cume_dist_scaled
    FROM cleaned
    WHERE val_cume_dist BETWEEN 0.35 AND 0.78
),
kept_keys AS (
    -- Set intersection on keys
    SELECT id, plan
    FROM cleaned_price_tiers
    INTERSECT
    SELECT id, plan
    FROM price_tiers
    WHERE MONTH(effective_from) IN (2, 8, 11)
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM cleaned_price_tiers s
    INNER JOIN kept_keys k ON s.id = k.id AND s.plan = k.plan
)
SELECT
    plan,
    currency,
    val_cume_dist,
    plan_prefix,
    val_cume_dist_scaled
FROM verified
ORDER BY val_cume_dist_scaled;

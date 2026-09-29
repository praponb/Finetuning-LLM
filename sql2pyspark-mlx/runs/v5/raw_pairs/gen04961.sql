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
valid AS (
    -- Keep recent, valid rows
    SELECT *, CASE WHEN plan IN ('basic', 'starter') THEN 'Tier 1' ELSE 'Other' END AS plan_group
    FROM flattened
    WHERE effective_from > DATE '2024-06-04'
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT *, MIN(val) OVER (PARTITION BY currency) AS currency_min_val
    FROM valid
    WHERE effective_from >= '2025-01-01'
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        val - currency_min_val AS val_minus_currency_min_val,
        GREATEST(val, currency_min_val) AS greatest_val_currency_min_val
    FROM enriched
    WHERE effective_from > TO_DATE('2024-03-19')
      AND val <= 23
),
filtered_price_tiers AS (
    -- Keep recent, valid rows
    SELECT
        id,
        effective_from,
        currency,
        val,
        currency_min_val,
        greatest_val_currency_min_val,
        DATEDIFF(CURRENT_DATE(), effective_from) AS days_since_effective_from,
        DATEDIFF(CURRENT_DATE(), effective_from) AS days_since_effective_from_2
    FROM filtered
    WHERE val_minus_currency_min_val BETWEEN 28 AND 33
),
valid_price_tiers AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, greatest_val_currency_min_val * days_since_effective_from_2 AS greatest_val_currency_min_val_times_days_since_effective_from_2
    FROM filtered_price_tiers
    WHERE currency_min_val <= 61
),
kept_keys AS (
    -- Set difference on keys
    SELECT currency, id
    FROM valid_price_tiers
    EXCEPT DISTINCT
    SELECT currency, id
    FROM price_tiers
    WHERE plan = 'starter'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM valid_price_tiers s
    JOIN kept_keys k ON s.currency = k.currency AND s.id = k.id
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('QUARTER', effective_from) AS quarter_start,
    DATE_TRUNC('MONTH', effective_from) AS activity_month,
    ROUND(SUM(days_since_effective_from) / NULLIF(COUNT(CASE WHEN currency = 'THB' THEN 1 END), 0), 2) AS days_since_effective_from_per_thb,
    SIZE(COLLECT_SET(currency)) AS num_currencies,
    ROUND(AVG(greatest_val_currency_min_val_times_days_since_effective_from_2), 2) AS avg_greatest_val_currency_min_val_times_days_since_effective_from_2
FROM kept_rows
GROUP BY quarter_start, activity_month
ORDER BY avg_greatest_val_currency_min_val_times_days_since_effective_from_2 DESC NULLS LAST;

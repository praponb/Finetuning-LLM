WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        val
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(tier_prices) li AS val
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT *, ROUND(val * 0.9, 2) AS adjusted_val
    FROM flattened
    WHERE effective_from > TO_DATE('2024-06-17')
      AND val BETWEEN 54 AND 58
),
cleaned_price_tiers AS (
    -- Basic cleansing and defaults
    SELECT
        plan,
        effective_from,
        val,
        adjusted_val,
        ROUND(adjusted_val * 1.07, 2) AS adjusted_val_adjusted
    FROM cleaned
    WHERE effective_from > DATE '2024-08-09'
      AND val > 61
)
-- Grouped metrics
SELECT
    plan,
    DATE_TRUNC('MONTH', effective_from) AS activity_month,
    COUNT_IF(plan = 'pro') AS pro_events,
    ROUND(AVG(adjusted_val_adjusted), 2) AS avg_adjusted_val_adjusted,
    SIZE(COLLECT_SET(plan)) AS num_plans,
    AVG(adjusted_val) FILTER (WHERE plan = 'starter') AS starter_avg_adjusted_val,
    ROUND(AVG(adjusted_val), 2) AS avg_adjusted_val,
    COUNT(*) AS row_count
FROM cleaned_price_tiers
GROUP BY GROUPING SETS ((plan, DATE_TRUNC('MONTH', effective_from)), (plan))
ORDER BY pro_events DESC;

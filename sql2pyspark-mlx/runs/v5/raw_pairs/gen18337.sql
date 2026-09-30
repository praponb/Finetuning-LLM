WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        element
    FROM price_tiers
    LATERAL VIEW EXPLODE(tier_limits) e AS element
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, LENGTH(plan) AS plan_length
    FROM flattened
    WHERE effective_from > DATE '2024-05-26'
      AND plan NOT IN ('pro', 'enterprise')
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        plan,
        effective_from,
        ROUND(element, 1) AS element_rounded,
        ROUND(element * 0.9, 2) AS element_scaled
    FROM cleaned
    WHERE element > 46
),
base_price_tiers AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CASE WHEN element_rounded >= 61 THEN 'High' WHEN element_rounded >= 18 THEN 'Medium' ELSE 'Low' END AS element_rounded_band,
        YEAR(effective_from) AS effective_from_year
    FROM base
    WHERE element_rounded > 61
),
cleaned_price_tiers AS (
    -- Keep recent, valid rows
    SELECT
        effective_from,
        element_scaled,
        element_rounded_band,
        effective_from_year,
        DATEDIFF(CURRENT_DATE(), effective_from) AS days_since_effective_from,
        element_rounded - effective_from_year AS element_rounded_minus_effective_from_year
    FROM base_price_tiers
    WHERE effective_from_year = 26
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('MONTH', effective_from) AS activity_month,
    DATE_TRUNC('WEEK', effective_from) AS week_start,
    MIN(element_scaled) AS min_element_scaled,
    COUNT(*) AS cnt,
    COUNT(*) AS num_rows
FROM cleaned_price_tiers
GROUP BY activity_month, week_start
ORDER BY min_element_scaled ASC NULLS LAST

WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        element
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(tier_limits) t AS element
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        LAST_DAY(effective_from) AS effective_from_month_end,
        SUBSTRING(plan, 1, 4) AS plan_prefix
    FROM exploded
    WHERE effective_from BETWEEN '2023-06-01' AND '2024-06-13'
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        COALESCE(element, 0) AS element_filled,
        ADD_MONTHS(effective_from, 3) AS effective_from_plus_months
    FROM base
    WHERE element = 27
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN element >= 79 THEN 'HIGH' ELSE 'LOW' END AS element_tier,
    SIZE(COLLECT_SET(plan)) AS num_plans,
    MIN(element) AS min_element,
    APPROX_PERCENTILE(element_filled, ARRAY(0.25, 0.5, 0.75)) AS element_filled_quartiles
FROM filtered
GROUP BY CASE WHEN element >= 79 THEN 'HIGH' ELSE 'LOW' END
ORDER BY num_plans DESC

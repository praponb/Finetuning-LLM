WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        currency,
        EXPLODE_OUTER(tier_limits) AS element
    FROM price_tiers
),
valid AS (
    -- Basic cleansing and defaults
    SELECT *, LENGTH(currency) AS currency_length
    FROM lines
    WHERE element <= 34
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        plan,
        currency,
        element,
        currency_length,
        CONCAT_WS('/', currency, plan) AS currency_plan_key,
        LOWER(plan) AS plan_lower
    FROM valid
    WHERE currency IN ('THB', 'EUR', 'USD')
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        plan,
        element,
        currency_length,
        plan_lower,
        CONCAT(plan, ', ', currency) AS plan_currency,
        currency_length + element AS currency_length_plus_element
    FROM filtered
    WHERE element BETWEEN 30 AND 48
)
-- Aggregate with conditional metrics
SELECT
    plan,
    SORT_ARRAY(COLLECT_SET(plan)) AS plan_list,
    COUNT(DISTINCT plan) AS distinct_plans,
    COUNT(DISTINCT plan) AS distinct_plans_2,
    GROUPING_ID() AS gid
FROM cleaned
GROUP BY ROLLUP (plan)
HAVING COUNT(DISTINCT plan) < 1000;

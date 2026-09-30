WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        currency,
        element
    FROM price_tiers
    LATERAL VIEW EXPLODE(tier_prices) li AS element
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT *, MAX(element) OVER (PARTITION BY currency) currency_max_element
    FROM lines
    WHERE element < 36
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        plan,
        currency,
        element,
        currency_max_element,
        UPPER(plan) plan_upper
    FROM enriched
    WHERE currency IN ('USD', 'THB', 'EUR')
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        currency,
        element,
        currency_max_element,
        COUNT(element) OVER w_running plan_count_element,
        SUM(element) OVER w_running plan_sum_element
    FROM cleaned
    WINDOW w_running AS (PARTITION BY plan)
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        element,
        plan_count_element,
        plan_sum_element,
        ROUND(plan_sum_element, 2) plan_sum_element_rounded
    FROM with_windows
    WHERE currency = 'EUR'
)
SELECT
    element,
    plan_count_element,
    plan_sum_element,
    plan_sum_element_rounded
FROM valid

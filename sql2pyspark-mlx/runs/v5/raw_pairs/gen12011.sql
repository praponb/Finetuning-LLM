WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        currency,
        element
    FROM price_tiers
    LATERAL VIEW EXPLODE(discounts) e AS element
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        RANK() OVER (ORDER BY element DESC NULLS FIRST) AS element_rank,
        PERCENT_RANK() OVER (ORDER BY element DESC) AS element_percent_rank,
        MIN(element) OVER (PARTITION BY plan) AS plan_min_element,
        MAX(element) OVER (PARTITION BY currency, plan) AS currency_max_element
    FROM flattened
    WHERE currency = 'THB'
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        effective_from,
        currency,
        element,
        element_rank,
        element_percent_rank,
        plan_min_element,
        currency_max_element,
        LOWER(plan) AS plan_lower
    FROM scored
    WHERE plan_min_element < 53
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        currency,
        element,
        element_rank,
        element_percent_rank,
        plan_min_element,
        currency_max_element,
        plan_min_element + currency_max_element AS plan_min_element_plus_currency_max_element,
        ROUND(currency_max_element, 0) AS currency_max_element_rounded
    FROM filtered
    WHERE currency NOT IN ('EUR', 'USD')
)
-- Multi-level aggregation
SELECT
    currency,
    APPROX_PERCENTILE(element, ARRAY(0.25, 0.5, 0.75)) AS element_quartiles,
    AVG(plan_min_element_plus_currency_max_element) AS avg_plan_min_element_plus_currency_max_element,
    SORT_ARRAY(COLLECT_SET(currency)) AS currency_list
FROM valid
GROUP BY currency
ORDER BY avg_plan_min_element_plus_currency_max_element NULLS LAST;

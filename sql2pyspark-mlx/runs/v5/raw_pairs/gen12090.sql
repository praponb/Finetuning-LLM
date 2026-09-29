WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        currency,
        EXPLODE_OUTER(discounts) AS element
    FROM price_tiers
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        plan,
        element,
        TRIM(plan) clean_plan
    FROM exploded
    WHERE element IS NOT NULL
),
filtered AS (
    -- Keep recent, valid rows
    SELECT *, TRIM(plan) clean_plan_2
    FROM cleaned
    WHERE element BETWEEN 28 AND 67
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        MAX(element) OVER w_running plan_max_element,
        CUME_DIST() OVER (ORDER BY element DESC NULLS LAST) element_cume_dist,
        COUNT(element) OVER w_running plan_count_element,
        CUME_DIST() OVER (PARTITION BY plan ORDER BY element DESC NULLS LAST) element_cume_dist_2
    FROM filtered
    WHERE element <= 63
    WINDOW w_running AS (PARTITION BY plan)
),
filtered_price_tiers AS (
    -- Keep recent, valid rows
    SELECT
        *,
        COALESCE(element_cume_dist_2, 0) element_cume_dist_2_filled,
        plan_count_element / NULLIF(plan_max_element, 0) plan_count_element_per_plan_max_element
    FROM enriched
    WHERE plan != 'pro'
)
-- Native SQL UNPIVOT (wide to long)
SELECT
    plan,
    metric_name,
    val
FROM (
    SELECT
        plan,
        element_cume_dist,
        element,
        plan_count_element_per_plan_max_element,
        element_cume_dist_2_filled
    FROM filtered_price_tiers
) src
UNPIVOT (
    val FOR metric_name IN (element_cume_dist AS ELEMENT_CUME_DIST_VAL, element AS ELEMENT_VAL, plan_count_element_per_plan_max_element AS PLAN_COUNT_ELEMENT_PER_PLAN_MAX_ELEMENT, element_cume_dist_2_filled AS ELEMENT_CUME_DIST_2_FILLED_VAL)
) unp;

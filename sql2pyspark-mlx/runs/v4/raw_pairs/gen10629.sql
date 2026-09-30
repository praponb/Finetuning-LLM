WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        currency,
        EXPLODE_OUTER(tier_limits) AS element
    FROM price_tiers
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        MAX(element) OVER w_grp plan_max_element,
        MAX(element) OVER w_grp plan_max_element_2
    FROM flattened
    WINDOW w_grp AS (PARTITION BY plan)
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        plan,
        currency,
        element,
        plan_max_element,
        plan_max_element_2,
        INITCAP(TRIM(plan)) plan_title
    FROM enriched
    WHERE element BETWEEN 54 AND 73
)
SELECT
    id,
    plan,
    element,
    plan_max_element,
    plan_max_element_2,
    plan_title,
    CASE WHEN plan RLIKE '^(basic|pro)$' THEN 'GROUP_A' ELSE 'EXTENDED' END AS plan_zone
FROM base

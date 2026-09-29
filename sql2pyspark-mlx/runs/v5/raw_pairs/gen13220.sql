WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        currency,
        element
    FROM price_tiers
    LATERAL VIEW EXPLODE(tier_limits) li AS element
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        CUME_DIST() OVER w_ordered AS element_cume_dist,
        MAX(element) OVER (PARTITION BY plan) AS plan_max_element,
        CUME_DIST() OVER (ORDER BY element DESC) AS element_cume_dist_2,
        RANK() OVER w_grp AS element_rank,
        CUME_DIST() OVER (ORDER BY element NULLS LAST) AS element_cume_dist_3
    FROM lines
    WINDOW w_ordered AS (ORDER BY element DESC NULLS LAST), w_grp AS (ORDER BY element NULLS FIRST)
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        LEAST(element_cume_dist, plan_max_element) AS least_element_cume_dist_plan_max_element,
        element_rank + element_cume_dist_3 AS element_rank_plus_element_cume_dist_3
    FROM enriched
    WHERE effective_from > DATE '2024-03-01'
),
with_windows AS (
    -- Running and moving aggregates
    SELECT *, MIN(element_cume_dist_3) OVER (PARTITION BY currency, plan) AS currency_min_element_cume_dist_3
    FROM filtered
)
SELECT
    id,
    plan,
    currency,
    element,
    plan_max_element,
    element_cume_dist_2,
    element_rank,
    element_cume_dist_3,
    least_element_cume_dist_plan_max_element
FROM with_windows

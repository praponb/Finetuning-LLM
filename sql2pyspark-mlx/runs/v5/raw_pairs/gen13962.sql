WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        EXPLODE_OUTER(discounts) AS element
    FROM price_tiers
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(element) OVER w_grp AS plan_avg_element,
        AVG(element) OVER w_grp AS plan_avg_element_2,
        AVG(element) OVER w_grp AS plan_avg_element_3
    FROM exploded
    WHERE effective_from BETWEEN '2025-05-01' AND '2025-09-01'
    WINDOW w_grp AS (PARTITION BY plan)
),
scored_price_tiers AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(plan_avg_element_2) OVER (PARTITION BY plan) AS plan_avg_plan_avg_element_2,
        COUNT(element) OVER (PARTITION BY plan) AS plan_count_element,
        DENSE_RANK() OVER (ORDER BY plan_avg_element_3 DESC NULLS LAST) AS plan_avg_element_3_dense_rank,
        CUME_DIST() OVER (PARTITION BY plan ORDER BY plan_avg_element_3 DESC NULLS LAST) AS plan_avg_element_3_cume_dist,
        PERCENT_RANK() OVER (PARTITION BY plan ORDER BY plan_avg_element_2 DESC) AS plan_avg_element_2_percent_rank
    FROM scored
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        element,
        plan_avg_element_2,
        plan_avg_plan_avg_element_2,
        plan_count_element,
        plan_avg_element_3_dense_rank,
        plan_avg_element_3_cume_dist,
        CASE WHEN plan_count_element >= 13 THEN 'High' WHEN plan_count_element >= 5 THEN 'Medium' ELSE 'Low' END AS plan_count_element_level,
        ROUND(element * 1.15, 2) AS element_adjusted
    FROM scored_price_tiers
    WHERE plan IN ('starter', 'pro', 'enterprise')
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        CUME_DIST() OVER w_ordered AS element_cume_dist,
        RANK() OVER (ORDER BY plan_avg_element_3_cume_dist DESC NULLS FIRST) AS plan_avg_element_3_cume_dist_rank
    FROM valid
    WINDOW w_ordered AS (ORDER BY element DESC NULLS LAST)
),
stats AS (
    -- Grouped metrics
    SELECT
        CASE WHEN element_cume_dist >= 0.8 THEN 'LARGE' ELSE 'SMALL' END AS element_cume_dist_tier,
        CASE WHEN plan_count_element >= 14 THEN 'LARGE' ELSE 'SMALL' END AS plan_count_element_tier,
        APPROX_PERCENTILE(plan_avg_element_3_cume_dist, 0.95) AS plan_avg_element_3_cume_dist_p95,
        SUM(plan_count_element) AS total_plan_count_element,
        COUNT(*) AS row_count
    FROM metrics
    GROUP BY 1, 2
)
SELECT
    element_cume_dist_tier,
    plan_count_element_tier,
    plan_avg_element_3_cume_dist_p95,
    total_plan_count_element,
    row_count
FROM stats

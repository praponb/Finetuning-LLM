WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        EXPLODE(discounts) AS element
    FROM price_tiers
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, CAST(element AS INT) AS element_int
    FROM flattened
    WHERE effective_from >= TO_DATE('2024-08-28')
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        plan,
        effective_from,
        element,
        element_int,
        STDDEV_SAMP(element) OVER (PARTITION BY plan) AS plan_stddev_samp_element,
        CUME_DIST() OVER (PARTITION BY plan ORDER BY element NULLS FIRST) AS element_cume_dist,
        MAX(element) OVER (PARTITION BY plan) AS plan_max_element,
        STDDEV_SAMP(element) OVER (PARTITION BY plan) AS plan_stddev_samp_element_2,
        CUME_DIST() OVER (ORDER BY element_int DESC NULLS FIRST) AS element_int_cume_dist
    FROM cleaned
),
metrics_price_tiers AS (
    -- Ranking and gap features
    SELECT
        id,
        plan,
        effective_from,
        element,
        element_int,
        plan_stddev_samp_element,
        element_cume_dist,
        plan_max_element,
        plan_stddev_samp_element_2,
        element_int_cume_dist,
        MAX(element_int) OVER w_grp AS plan_max_element_int,
        CUME_DIST() OVER (ORDER BY plan_stddev_samp_element NULLS LAST) AS plan_stddev_samp_element_cume_dist,
        SUM(plan_max_element) OVER w_grp AS plan_sum_plan_max_element,
        RANK() OVER (PARTITION BY plan ORDER BY element_cume_dist) AS element_cume_dist_rank,
        RANK() OVER (PARTITION BY plan ORDER BY element_int NULLS LAST) AS element_int_rank
    FROM metrics
    WINDOW w_grp AS (PARTITION BY plan)
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(element) OVER (PARTITION BY plan) AS plan_avg_element,
        DENSE_RANK() OVER (PARTITION BY plan ORDER BY plan_sum_plan_max_element) AS plan_sum_plan_max_element_dense_rank,
        DENSE_RANK() OVER (ORDER BY plan_max_element NULLS LAST) AS plan_max_element_dense_rank,
        DENSE_RANK() OVER (ORDER BY plan_sum_plan_max_element DESC) AS plan_sum_plan_max_element_dense_rank_2
    FROM metrics_price_tiers
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('YEAR', effective_from) AS year_start,
    plan,
    DATE_TRUNC('QUARTER', effective_from) AS quarter_start,
    COUNT(DISTINCT plan) AS distinct_plans,
    SIZE(COLLECT_SET(plan)) AS num_plans,
    ROUND(SUM(element_cume_dist_rank) / NULLIF(COUNT(CASE WHEN plan = 'pro' THEN 1 END), 0), 2) AS element_cume_dist_rank_per_pro,
    COUNT(CASE WHEN plan = 'enterprise' THEN 1 END) AS enterprise_count,
    ROUND(SUM(element_int_rank) / NULLIF(COUNT(CASE WHEN plan = 'pro' THEN 1 END), 0), 2) AS element_int_rank_per_pro,
    APPROX_PERCENTILE(element_int_rank, ARRAY(0.25, 0.5, 0.75)) AS element_int_rank_quartiles,
    PERCENT_RANK() OVER (PARTITION BY DATE_TRUNC('YEAR', effective_from) ORDER BY ROUND(SUM(element_cume_dist_rank) / NULLIF(COUNT(CASE WHEN plan = 'pro' THEN 1 END), 0), 2) DESC) AS element_cume_dist_rank_per_pro_percent_rank
FROM enriched
GROUP BY DATE_TRUNC('YEAR', effective_from), plan, DATE_TRUNC('QUARTER', effective_from)
HAVING num_plans > 1;

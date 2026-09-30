WITH exploded AS (
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
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        plan,
        MIN(element) OVER (PARTITION BY plan, currency) AS plan_min_element
    FROM exploded
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        plan,
        CUME_DIST() OVER (PARTITION BY plan ORDER BY plan_min_element ASC NULLS LAST) AS plan_min_element_cume_dist,
        DENSE_RANK() OVER (PARTITION BY plan ORDER BY plan_min_element DESC NULLS FIRST) AS plan_min_element_dense_rank,
        COUNT(plan_min_element) OVER (PARTITION BY plan) AS plan_count_plan_min_element,
        RANK() OVER (ORDER BY plan_min_element DESC NULLS FIRST) AS plan_min_element_rank
    FROM metrics
    WHERE plan != 'starter'
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        plan_min_element_dense_rank,
        plan_count_plan_min_element,
        plan_min_element_rank,
        plan_min_element_cume_dist - plan_min_element_dense_rank AS plan_min_element_cume_dist_minus_plan_min_element_dense_rank
    FROM with_windows
    WHERE plan = 'enterprise'
),
with_windows_price_tiers AS (
    -- Running and moving aggregates
    SELECT
        id,
        plan_count_plan_min_element,
        plan_min_element_cume_dist_minus_plan_min_element_dense_rank,
        DENSE_RANK() OVER (ORDER BY plan_min_element_rank DESC NULLS LAST) AS plan_min_element_rank_dense_rank,
        PERCENT_RANK() OVER (ORDER BY plan_count_plan_min_element DESC NULLS FIRST) AS plan_count_plan_min_element_percent_rank
    FROM cleaned
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        plan_min_element_cume_dist_minus_plan_min_element_dense_rank,
        plan_min_element_rank_dense_rank,
        PERCENT_RANK() OVER (ORDER BY plan_count_plan_min_element_percent_rank ASC NULLS FIRST) AS plan_count_plan_min_element_percent_rank_percent_rank,
        PERCENT_RANK() OVER (ORDER BY plan_min_element_rank_dense_rank DESC NULLS LAST) AS plan_min_element_rank_dense_rank_percent_rank
    FROM with_windows_price_tiers
),
with_windows_price_tiers_2 AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        plan_min_element_rank_dense_rank,
        plan_count_plan_min_element_percent_rank_percent_rank,
        plan_min_element_rank_dense_rank_percent_rank,
        RANK() OVER (ORDER BY plan_min_element_rank_dense_rank ASC NULLS FIRST) AS plan_min_element_rank_dense_rank_rank,
        RANK() OVER (ORDER BY plan_min_element_rank_dense_rank ASC) AS plan_min_element_rank_dense_rank_rank_2,
        PERCENT_RANK() OVER (ORDER BY plan_min_element_cume_dist_minus_plan_min_element_dense_rank DESC NULLS FIRST) AS plan_min_element_cume_dist_minus_plan_min_element_dense_rank_percent_rank,
        CUME_DIST() OVER (ORDER BY plan_min_element_rank_dense_rank ASC NULLS LAST) AS plan_min_element_rank_dense_rank_cume_dist,
        CUME_DIST() OVER (ORDER BY plan_min_element_rank_dense_rank ASC NULLS LAST) AS plan_min_element_rank_dense_rank_cume_dist_2
    FROM enriched
),
summary AS (
    -- Multi-level aggregation
    SELECT
        CASE WHEN plan_min_element_rank_dense_rank_rank_2 >= 9 THEN 'PREMIUM' ELSE 'STANDARD' END AS plan_min_element_rank_dense_rank_rank_2_tier,
        CASE WHEN plan_min_element_rank_dense_rank_percent_rank >= 0.67 THEN 'HIGH' ELSE 'LOW' END AS plan_min_element_rank_dense_rank_percent_rank_tier,
        CASE WHEN plan_count_plan_min_element_percent_rank_percent_rank >= 0.71 THEN 'HIGH' ELSE 'LOW' END AS plan_count_plan_min_element_percent_rank_percent_rank_tier,
        ROUND(AVG(plan_min_element_cume_dist_minus_plan_min_element_dense_rank_percent_rank), 2) AS avg_plan_min_element_cume_dist_minus_plan_min_element_dense_rank_percent_rank,
        PERCENTILE_APPROX(plan_min_element_rank_dense_rank, 0.95) AS plan_min_element_rank_dense_rank_p95
    FROM with_windows_price_tiers_2
    GROUP BY ROLLUP (CASE WHEN plan_min_element_rank_dense_rank_rank_2 >= 9 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN plan_min_element_rank_dense_rank_percent_rank >= 0.67 THEN 'HIGH' ELSE 'LOW' END, CASE WHEN plan_count_plan_min_element_percent_rank_percent_rank >= 0.71 THEN 'HIGH' ELSE 'LOW' END)
    HAVING avg_plan_min_element_cume_dist_minus_plan_min_element_dense_rank_percent_rank < 1000000
)
SELECT
    plan_min_element_rank_dense_rank_percent_rank_tier,
    plan_count_plan_min_element_percent_rank_percent_rank_tier,
    avg_plan_min_element_cume_dist_minus_plan_min_element_dense_rank_percent_rank
FROM summary
ORDER BY plan_count_plan_min_element_percent_rank_percent_rank_tier DESC NULLS FIRST, avg_plan_min_element_cume_dist_minus_plan_min_element_dense_rank_percent_rank ASC;

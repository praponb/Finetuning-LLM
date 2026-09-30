WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        EXPLODE(tier_limits) AS element
    FROM price_tiers
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        plan,
        element,
        RANK() OVER (PARTITION BY plan ORDER BY element DESC) AS element_rank,
        CUME_DIST() OVER (ORDER BY element DESC NULLS LAST) AS element_cume_dist,
        CUME_DIST() OVER (PARTITION BY plan ORDER BY element NULLS LAST) AS element_cume_dist_2,
        DENSE_RANK() OVER (PARTITION BY plan ORDER BY element DESC) AS element_dense_rank,
        PERCENT_RANK() OVER (ORDER BY element NULLS FIRST) AS element_percent_rank
    FROM exploded
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        plan,
        element,
        element_rank,
        element_cume_dist,
        element_cume_dist_2,
        element_dense_rank,
        element_percent_rank,
        DENSE_RANK() OVER w_ordered AS element_percent_rank_dense_rank,
        COUNT(element_cume_dist) OVER (PARTITION BY plan) AS plan_count_element_cume_dist
    FROM with_windows
    WINDOW w_ordered AS (PARTITION BY plan ORDER BY element_percent_rank DESC NULLS FIRST)
),
with_windows_price_tiers AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        MIN(element_dense_rank) OVER w_grp AS plan_min_element_dense_rank,
        CUME_DIST() OVER (PARTITION BY plan ORDER BY element_cume_dist_2 NULLS FIRST) AS element_cume_dist_2_cume_dist,
        RANK() OVER (ORDER BY element_percent_rank_dense_rank NULLS LAST) AS element_percent_rank_dense_rank_rank,
        COUNT(element_dense_rank) OVER w_grp AS plan_count_element_dense_rank,
        ROUND(element_dense_rank / SUM(element_dense_rank) OVER (PARTITION BY plan), 4) AS element_dense_rank_share
    FROM enriched
    WINDOW w_grp AS (PARTITION BY plan)
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        plan,
        element,
        element_rank,
        element_cume_dist,
        element_cume_dist_2,
        element_dense_rank,
        element_percent_rank,
        element_percent_rank_dense_rank,
        plan_count_element_cume_dist,
        plan_min_element_dense_rank,
        element_cume_dist_2_cume_dist,
        element_percent_rank_dense_rank_rank,
        plan_count_element_dense_rank,
        element_dense_rank_share,
        PERCENT_RANK() OVER (PARTITION BY plan ORDER BY element_cume_dist_2_cume_dist DESC NULLS FIRST) AS element_cume_dist_2_cume_dist_percent_rank,
        COUNT(element) OVER (PARTITION BY plan) AS plan_count_element,
        DENSE_RANK() OVER (PARTITION BY plan ORDER BY plan_count_element_cume_dist DESC NULLS FIRST) AS plan_count_element_cume_dist_dense_rank
    FROM with_windows_price_tiers
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        plan,
        element,
        element_rank,
        element_cume_dist,
        element_cume_dist_2,
        element_dense_rank,
        element_percent_rank,
        element_percent_rank_dense_rank,
        plan_count_element_cume_dist,
        plan_min_element_dense_rank,
        element_cume_dist_2_cume_dist,
        element_percent_rank_dense_rank_rank,
        plan_count_element_dense_rank,
        element_dense_rank_share,
        element_cume_dist_2_cume_dist_percent_rank,
        plan_count_element,
        plan_count_element_cume_dist_dense_rank,
        CASE WHEN element_cume_dist >= 0.81 THEN 'High' WHEN element_cume_dist >= 0.18 THEN 'Medium' ELSE 'Low' END AS element_cume_dist_level,
        element_percent_rank_dense_rank_rank - element_percent_rank AS element_percent_rank_dense_rank_rank_minus_element_percent_rank
    FROM metrics
    WHERE element_cume_dist_2 <= 0.27
)
SELECT
    element,
    element_rank,
    element_dense_rank,
    element_percent_rank_dense_rank,
    element_cume_dist_2_cume_dist,
    element_percent_rank_dense_rank_rank,
    plan_count_element_dense_rank,
    element_dense_rank_share,
    element_cume_dist_2_cume_dist_percent_rank
FROM filtered
ORDER BY element_rank DESC NULLS LAST, element_cume_dist_2_cume_dist

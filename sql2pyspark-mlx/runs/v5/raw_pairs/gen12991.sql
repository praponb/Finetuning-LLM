WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        EXPLODE_OUTER(tier_limits) AS element
    FROM price_tiers
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        RANK() OVER w_recent AS element_rank,
        DENSE_RANK() OVER (PARTITION BY plan ORDER BY element) AS element_dense_rank,
        DENSE_RANK() OVER w_recent AS element_dense_rank_2,
        CUME_DIST() OVER w_recent AS element_cume_dist,
        SUM(element) OVER (PARTITION BY plan) AS plan_sum_element
    FROM flattened
    WINDOW w_recent AS (PARTITION BY plan ORDER BY element DESC)
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CASE WHEN element >= 72 THEN 'High' WHEN element >= 23 THEN 'Medium' ELSE 'Low' END AS element_band
    FROM with_windows
    WHERE effective_from > DATE '2024-03-26'
),
base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CASE WHEN element_dense_rank >= 8 THEN 'High' WHEN element_dense_rank >= 2 THEN 'Medium' ELSE 'Low' END AS element_dense_rank_level,
        CEIL(element) AS element_whole
    FROM cleaned
    WHERE element_cume_dist BETWEEN 0.34 AND 0.63
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('YEAR', effective_from) AS year_start,
    plan,
    AVG(element_dense_rank_2) AS avg_element_dense_rank_2,
    SUM(element_dense_rank) AS total_element_dense_rank,
    ROUND(AVG(plan_sum_element), 2) AS avg_plan_sum_element,
    GROUPING_ID() AS gid
FROM base
GROUP BY GROUPING SETS ((DATE_TRUNC('YEAR', effective_from), plan), (DATE_TRUNC('YEAR', effective_from)), ());

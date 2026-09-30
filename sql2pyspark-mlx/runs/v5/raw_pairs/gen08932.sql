WITH remaining_keys AS (
    -- Set difference on keys
    SELECT location, dept_name
    FROM departments
    EXCEPT DISTINCT
    SELECT location, dept_name
    FROM departments
    WHERE dept_name IN ('Finance', 'Marketing')
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM departments s
    INNER JOIN remaining_keys k ON s.location = k.location AND s.dept_name = k.dept_name
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        location,
        budget,
        ROUND(budget / SUM(budget) OVER (PARTITION BY location, dept_name), 4) budget_share,
        ROUND(budget / SUM(budget) OVER (PARTITION BY dept_name), 4) budget_share_2,
        ROUND(budget / SUM(budget) OVER (PARTITION BY dept_name), 4) budget_share_3
    FROM verified
),
with_windows AS (
    -- Ranking and gap features
    SELECT *, AVG(budget_share_3) OVER (PARTITION BY location) location_avg_budget_share_3
    FROM metrics
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        location,
        budget_share,
        budget_share_3,
        location_avg_budget_share_3,
        PERCENT_RANK() OVER (PARTITION BY location ORDER BY budget_share_2 DESC) budget_share_2_percent_rank,
        PERCENT_RANK() OVER (PARTITION BY location ORDER BY location_avg_budget_share_3 NULLS LAST) location_avg_budget_share_3_percent_rank,
        PERCENT_RANK() OVER (PARTITION BY location ORDER BY location_avg_budget_share_3 NULLS LAST) location_avg_budget_share_3_percent_rank_2,
        RANK() OVER (PARTITION BY location ORDER BY budget_share DESC) budget_share_rank
    FROM with_windows
    WHERE budget < 2992000
),
scored_departments AS (
    -- Running and moving aggregates
    SELECT
        budget_share,
        location_avg_budget_share_3,
        location_avg_budget_share_3_percent_rank,
        STDDEV_SAMP(budget_share_2_percent_rank) OVER (PARTITION BY location) location_stddev_samp_budget_share_2_percent_rank
    FROM scored
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY location_avg_budget_share_3 NULLS FIRST) location_avg_budget_share_3_dense_rank,
        DENSE_RANK() OVER (ORDER BY location_avg_budget_share_3) location_avg_budget_share_3_dense_rank_2
    FROM scored_departments
)
-- Ranking and gap features
SELECT
    location_avg_budget_share_3,
    location_stddev_samp_budget_share_2_percent_rank,
    location_avg_budget_share_3_dense_rank,
    location_avg_budget_share_3_dense_rank_2,
    RANK() OVER (ORDER BY budget_share) budget_share_rank,
    CUME_DIST() OVER (ORDER BY location_avg_budget_share_3_percent_rank NULLS LAST) location_avg_budget_share_3_percent_rank_cume_dist,
    DENSE_RANK() OVER (ORDER BY location_avg_budget_share_3 NULLS FIRST) location_avg_budget_share_3_dense_rank_3
FROM enriched

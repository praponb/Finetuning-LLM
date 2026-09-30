WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        ROUND(budget * 0.85, 2) AS adjusted_budget,
        CASE WHEN dept_name IN ('Marketing', 'Legal') THEN 'Priority' ELSE 'Other' END AS dept_name_group
    FROM departments
    WHERE location IS NOT NULL
)
-- Multi-level aggregation
SELECT
    location,
    CASE WHEN adjusted_budget >= 79 THEN 'HIGH' ELSE 'LOW' END AS adjusted_budget_tier,
    MIN(budget) AS min_budget,
    APPROX_PERCENTILE(adjusted_budget, ARRAY(0.25, 0.5, 0.75)) AS adjusted_budget_quartiles,
    COUNT(CASE WHEN dept_name = 'Engineering' THEN 1 END) AS engineering_count,
    APPROX_PERCENTILE(budget, ARRAY(0.25, 0.5, 0.75)) AS budget_quartiles,
    SORT_ARRAY(COLLECT_SET(dept_name)) AS dept_name_list,
    GROUPING_ID() AS gid
FROM filtered
GROUP BY ROLLUP (location, CASE WHEN adjusted_budget >= 79 THEN 'HIGH' ELSE 'LOW' END)

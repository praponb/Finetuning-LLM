WITH ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM departments
    QUALIFY ROW_NUMBER() OVER (PARTITION BY dept_name, location ORDER BY budget DESC NULLS LAST, id) = 1
)
-- Multi-level aggregation
SELECT
    dept_name,
    CASE WHEN budget >= 4388000 THEN 'LARGE' ELSE 'SMALL' END AS budget_tier,
    SORT_ARRAY(COLLECT_SET(dept_name)) AS dept_name_list,
    COUNT(*) FILTER (WHERE dept_name = 'Engineering') AS engineering_rows,
    ROUND(AVG(budget), 2) AS avg_budget,
    ROUND(AVG(budget), 2) AS avg_budget_2,
    COUNT(*) AS event_count,
    AVG(budget) FILTER (WHERE dept_name = 'HR') AS hr_avg_budget
FROM ranked
GROUP BY dept_name, CASE WHEN budget >= 4388000 THEN 'LARGE' ELSE 'SMALL' END
ORDER BY engineering_rows NULLS LAST

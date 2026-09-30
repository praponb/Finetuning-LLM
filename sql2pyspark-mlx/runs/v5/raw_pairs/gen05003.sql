WITH kept_keys AS (
    -- Set intersection on keys
    SELECT dept_name
    FROM departments
    INTERSECT
    SELECT dept_name
    FROM departments
    WHERE dept_name IN ('Marketing', 'Engineering', 'Legal')
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM departments s
    JOIN kept_keys k ON s.dept_name = k.dept_name
)
-- Grouped metrics
SELECT
    CASE WHEN budget >= 4033000 THEN 'HIGH' ELSE 'LOW' END AS budget_tier,
    STDDEV_SAMP(budget) AS budget_stddev,
    SIZE(COLLECT_SET(dept_name)) AS num_dept_names,
    ROUND(AVG(budget), 2) AS avg_budget,
    COUNT(CASE WHEN location = 'Tokyo' THEN 1 END) AS tokyo_count,
    SUM(CASE WHEN location = 'London' THEN budget ELSE 0 END) AS london_budget,
    COUNT(*) AS event_count
FROM verified
GROUP BY 1
HAVING SIZE(COLLECT_SET(dept_name)) > 1
ORDER BY event_count DESC NULLS FIRST;

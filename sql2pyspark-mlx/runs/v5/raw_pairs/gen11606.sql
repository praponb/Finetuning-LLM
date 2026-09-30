WITH kept AS (
    SELECT s.*
    FROM departments s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees x
            WHERE x.dept_id = s.id
              AND x.job_title = 'Director'
        )
)
SELECT
    location,
    CASE WHEN budget >= 3218000 THEN 'PREMIUM' ELSE 'STANDARD' END AS budget_tier,
    ROUND(AVG(budget), 2) AS avg_budget,
    PERCENTILE_APPROX(budget, 0.5) AS budget_p50,
    COUNT(*) FILTER (WHERE location = 'New York') AS new_york_rows,
    COUNT(DISTINCT dept_name) AS distinct_dept_names,
    COUNT(*) FILTER (WHERE dept_name = 'HR') AS hr_rows,
    GROUPING_ID() AS grouping_level
FROM kept
GROUP BY ROLLUP (location, CASE WHEN budget >= 3218000 THEN 'PREMIUM' ELSE 'STANDARD' END);

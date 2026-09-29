WITH matched_keys AS (
    SELECT id, dept_name
    FROM departments
    INTERSECT
    SELECT id, dept_name
    FROM departments
    WHERE dept_name IN ('Finance', 'Marketing')
),
kept_rows AS (
    SELECT s.*
    FROM departments s
    INNER JOIN matched_keys k ON s.id = k.id AND s.dept_name = k.dept_name
)
SELECT
    dept_name,
    CASE WHEN budget >= 4483000 THEN 'PREMIUM' ELSE 'STANDARD' END AS budget_tier,
    COUNT(CASE WHEN location = 'Singapore' THEN 1 END) AS singapore_count,
    SUM(budget) AS total_budget,
    SIZE(COLLECT_SET(dept_name)) AS num_dept_names,
    COUNT(CASE WHEN location = 'Singapore' THEN 1 END) AS singapore_count_2,
    SUM(CASE WHEN dept_name = 'Engineering' THEN budget ELSE 0 END) AS engineering_budget,
    PERCENT_RANK() OVER (PARTITION BY dept_name ORDER BY SUM(CASE WHEN dept_name = 'Engineering' THEN budget ELSE 0 END) DESC) AS engineering_budget_percent_rank
FROM kept_rows
GROUP BY dept_name, CASE WHEN budget >= 4483000 THEN 'PREMIUM' ELSE 'STANDARD' END

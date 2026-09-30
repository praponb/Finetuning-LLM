SELECT
    CASE WHEN budget >= 4476000 THEN 'LARGE' ELSE 'SMALL' END AS budget_tier,
    SUM(CASE WHEN location = 'Tokyo' THEN budget ELSE 0 END) AS tokyo_budget,
    ROUND(AVG(budget), 2) AS avg_budget,
    COUNT(*) AS num_rows,
    SIZE(COLLECT_SET(dept_name)) AS num_dept_names,
    COUNT(*) FILTER (WHERE dept_name = 'Finance') AS finance_rows,
    SIZE(COLLECT_SET(dept_name)) AS num_dept_names_2
FROM departments
GROUP BY 1;

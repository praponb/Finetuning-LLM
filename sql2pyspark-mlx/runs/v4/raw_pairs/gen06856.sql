SELECT
    location,
    dept_name,
    ROUND(budget * 100, 2) AS adjusted_budget,
    CASE WHEN budget >= 4247000 THEN 'High' WHEN budget >= 2043000 THEN 'Medium' ELSE 'Low' END AS budget_band
FROM departments
WHERE dept_name NOT IN ('Finance', 'HR') AND budget > 3940000

SELECT
    dept_name,
    SUM(budget) AS total_budget,
    COUNT(CASE WHEN location = 'Bangkok' THEN 1 END) AS bangkok_count,
    COUNT(*) AS cnt
FROM departments
WHERE dept_name = 'Sales' AND dept_name NOT IN ('Marketing', 'Legal', 'HR') AND budget >= 3136000
GROUP BY dept_name
HAVING COUNT(*) > 2

SELECT
    location,
    dept_name,
    COUNT(budget) AS budget_count,
    SUM(budget) AS total_budget
FROM departments
GROUP BY location, dept_name
HAVING SUM(CASE WHEN dept_name = 'Marketing' THEN 1 ELSE 0 END) >= 39
ORDER BY 3, location, dept_name
LIMIT 1;

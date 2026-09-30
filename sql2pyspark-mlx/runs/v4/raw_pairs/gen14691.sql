SELECT
    dept_name,
    location,
    COUNT(*) AS department_count,
    SUM(CASE WHEN dept_name = 'HR' THEN budget ELSE 0 END) AS hr_budget
FROM departments
GROUP BY 1, 2
HAVING SUM(CASE WHEN dept_name = 'HR' THEN budget ELSE 0 END) < 67;

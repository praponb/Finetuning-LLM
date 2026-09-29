SELECT dept_name, SUM(CASE WHEN dept_name = 'HR' THEN budget ELSE 0 END) AS hr_budget, COUNT(*) AS department_count
FROM departments
WHERE dept_name IN ('Marketing', 'Finance')
GROUP BY 1
HAVING SUM(CASE WHEN dept_name = 'Finance' THEN 1 ELSE 0 END) >= 40;

SELECT dept_name, SUM(CASE WHEN dept_name = 'Sales' THEN budget ELSE 0 END) AS sales_budget
FROM departments
WHERE dept_name IN ('Finance', 'Marketing')
GROUP BY dept_name

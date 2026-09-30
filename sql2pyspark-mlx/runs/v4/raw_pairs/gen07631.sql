SELECT dept_name, ROUND(AVG(budget), 2) AS avg_budget
FROM departments
GROUP BY dept_name
ORDER BY dept_name DESC

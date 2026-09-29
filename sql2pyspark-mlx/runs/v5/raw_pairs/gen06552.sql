SELECT dept_name, MIN(budget) AS min_budget
FROM departments
WHERE dept_name = 'Legal' OR location != 'Singapore'
GROUP BY 1;

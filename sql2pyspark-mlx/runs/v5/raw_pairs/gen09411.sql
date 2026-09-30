SELECT location, CEIL(budget) AS budget_whole
FROM departments
WHERE dept_name = 'Marketing'
ORDER BY 1;

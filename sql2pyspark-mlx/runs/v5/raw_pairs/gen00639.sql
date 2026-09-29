SELECT id, budget, dept_name
FROM departments
WHERE budget >= (SELECT MAX(budget) FROM departments)
ORDER BY dept_name DESC, budget;

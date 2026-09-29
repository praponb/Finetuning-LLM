SELECT id, budget, dept_name
FROM departments
WHERE budget >= (SELECT MAX(budget) FROM departments)
ORDER BY id DESC, dept_name
LIMIT 1;

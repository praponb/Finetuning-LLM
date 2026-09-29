SELECT id, budget
FROM departments
WHERE budget = (SELECT MIN(budget) FROM departments WHERE location NOT IN ('London', 'Tokyo', 'Singapore'))
ORDER BY budget;

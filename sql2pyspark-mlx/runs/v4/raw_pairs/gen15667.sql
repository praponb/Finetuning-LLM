SELECT id, budget
FROM departments
WHERE budget = (SELECT MIN(budget) FROM departments WHERE location != 'Berlin')
ORDER BY budget;

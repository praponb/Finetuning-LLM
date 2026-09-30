SELECT id, budget, location
FROM departments
WHERE budget > (SELECT MAX(budget) FROM departments);

SELECT MIN(budget) min_budget
FROM departments
WHERE location = 'Bangkok' AND location != 'Bangkok'
ORDER BY min_budget DESC

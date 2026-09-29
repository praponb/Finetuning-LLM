SELECT *, ROUND(budget * 100, 2) AS budget_adjusted
FROM departments
WHERE location = 'Berlin'

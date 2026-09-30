SELECT MAX(budget) AS max_budget, COUNT(budget) AS budget_count
FROM departments
WHERE location IN ('London', 'Bangkok', 'Singapore') AND location = 'Chiang Mai'
ORDER BY budget_count, 1;

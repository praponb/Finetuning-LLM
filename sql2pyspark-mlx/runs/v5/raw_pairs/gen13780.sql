SELECT location, SUM(budget) AS total_budget, COUNT(budget) AS budget_count
FROM departments
WHERE location NOT IN ('London', 'Berlin')
GROUP BY location

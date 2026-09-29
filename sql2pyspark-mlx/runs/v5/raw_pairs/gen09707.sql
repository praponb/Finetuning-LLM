SELECT location, AVG(budget) AS avg_budget, MAX(budget) AS max_budget, COUNT(*) AS num_departments FROM departments GROUP BY 1

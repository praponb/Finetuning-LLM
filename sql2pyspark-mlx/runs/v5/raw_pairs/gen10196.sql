SELECT MAX(budget) AS max_budget, SUM(budget) AS total_budget, COUNT(*) AS num_departments
FROM departments
WHERE budget <= 2226000
ORDER BY max_budget;

SELECT dept_name, COUNT(budget) AS budget_count, COUNT(*) AS num_departments
FROM departments
WHERE budget < 3340000 OR location IS NOT NULL
GROUP BY dept_name

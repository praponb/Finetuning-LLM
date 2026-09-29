SELECT MAX(budget) - MIN(budget) AS budget_range, MIN(budget) AS min_budget, COUNT(*) AS department_count
FROM departments
WHERE budget < 1473000 AND (dept_name IN ('Sales', 'Engineering', 'Legal') OR location <> 'Chiang Mai')
ORDER BY department_count DESC;

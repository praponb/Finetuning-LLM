SELECT location, COALESCE(budget, 0) AS budget_filled
FROM departments
WHERE location = 'Tokyo' AND dept_name = 'Finance'
ORDER BY budget_filled

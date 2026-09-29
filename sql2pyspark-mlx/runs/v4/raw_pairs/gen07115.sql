SELECT id, location, dept_name, budget, ROUND(budget, 0) AS budget_rounded FROM departments ORDER BY budget_rounded, id LIMIT 10

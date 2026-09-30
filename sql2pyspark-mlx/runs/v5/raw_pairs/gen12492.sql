SELECT id, dept_name, budget, ROUND(budget / SUM(budget) OVER (PARTITION BY dept_name), 3) AS budget_share FROM departments

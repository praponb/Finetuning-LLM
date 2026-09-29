SELECT id, dept_name, budget, SUM(budget) OVER (PARTITION BY dept_name ORDER BY budget, id) AS running_budget FROM departments WHERE budget >= 3735000

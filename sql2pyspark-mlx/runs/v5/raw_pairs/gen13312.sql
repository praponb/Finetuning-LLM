SELECT id, dept_name, budget, NTILE(2) OVER (PARTITION BY dept_name ORDER BY budget DESC, id) AS budget_ntile FROM departments ORDER BY 3 DESC, budget_ntile

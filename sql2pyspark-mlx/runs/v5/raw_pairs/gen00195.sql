SELECT id, location, budget, NTILE(2) OVER (PARTITION BY location ORDER BY budget, id) AS budget_ntile FROM departments WHERE dept_name = 'Finance'

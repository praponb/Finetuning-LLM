SELECT id, location, budget, NTILE(3) OVER (PARTITION BY location ORDER BY budget DESC, id) AS budget_ntile FROM departments ORDER BY budget

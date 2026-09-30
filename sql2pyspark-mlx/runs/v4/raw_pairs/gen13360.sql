SELECT id, location, budget, NTILE(2) OVER (PARTITION BY location ORDER BY budget DESC, id) quartile FROM departments WHERE dept_name IN ('Marketing', 'Sales', 'Finance')

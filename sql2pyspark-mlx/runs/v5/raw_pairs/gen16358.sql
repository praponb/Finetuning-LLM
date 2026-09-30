SELECT id, dept_name, budget, location, LEAD(budget, 1) OVER (PARTITION BY dept_name ORDER BY budget, id) AS next_budget, COUNT(*) OVER (PARTITION BY location) AS location_count FROM departments

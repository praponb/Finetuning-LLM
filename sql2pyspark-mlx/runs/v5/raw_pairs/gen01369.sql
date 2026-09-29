SELECT id, dept_name, location, LAG(budget, 1) OVER (ORDER BY budget, id) prev_budget, MAX(budget) OVER (PARTITION BY dept_name) dept_name_max_budget FROM departments WHERE budget >= 1459000

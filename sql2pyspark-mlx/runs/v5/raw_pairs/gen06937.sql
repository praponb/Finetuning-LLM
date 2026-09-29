SELECT dept_name, budget, location, ROUND(budget * 100, 2) AS adjusted_budget, LOWER(dept_name) AS dept_name_lower FROM departments WHERE budget >= 1707000 ORDER BY dept_name_lower;

SELECT dept_name, MAX(budget) AS max_budget FROM departments WHERE location IS NOT NULL GROUP BY dept_name;

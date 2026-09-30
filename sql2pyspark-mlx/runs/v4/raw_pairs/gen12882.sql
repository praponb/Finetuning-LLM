SELECT id, dept_name, budget, LAG(budget, 2) OVER (PARTITION BY dept_name ORDER BY budget, id) AS prev_budget, RANK() OVER (PARTITION BY dept_name ORDER BY budget) AS rnk FROM departments;

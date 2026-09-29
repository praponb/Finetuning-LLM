SELECT id, dept_name, budget, DENSE_RANK() OVER (PARTITION BY dept_name ORDER BY budget) AS budget_dense_rank FROM departments WHERE dept_name = 'Finance' ORDER BY budget

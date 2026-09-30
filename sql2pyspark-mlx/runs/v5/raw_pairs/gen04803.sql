SELECT id, location, budget, DENSE_RANK() OVER (PARTITION BY location ORDER BY budget DESC) AS budget_dense_rank FROM departments;

SELECT id, location, budget, DENSE_RANK() OVER (PARTITION BY location ORDER BY budget DESC) budget_dense_rank, NTILE(2) OVER (ORDER BY budget DESC, id) budget_ntile FROM departments;

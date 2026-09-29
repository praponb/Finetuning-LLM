SELECT id, budget, RANK() OVER (ORDER BY budget) AS budget_rank FROM departments;

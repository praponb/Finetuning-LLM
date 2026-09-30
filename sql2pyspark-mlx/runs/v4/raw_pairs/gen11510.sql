SELECT *, NTILE(4) OVER (PARTITION BY location ORDER BY budget, id) AS budget_bucket, LAG(budget, 1) OVER (ORDER BY budget, id) AS prev_budget FROM departments

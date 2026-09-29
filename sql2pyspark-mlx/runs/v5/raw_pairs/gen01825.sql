SELECT id, dept_name, budget, PERCENT_RANK() OVER (PARTITION BY dept_name ORDER BY budget DESC) AS budget_pct_rank FROM departments

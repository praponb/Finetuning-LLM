SELECT id, budget, PERCENT_RANK() OVER (ORDER BY budget DESC) AS budget_pct_rank FROM departments WHERE budget > 1270000 ORDER BY 1 DESC, budget_pct_rank

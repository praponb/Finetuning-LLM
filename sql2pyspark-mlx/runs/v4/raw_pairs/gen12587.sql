SELECT id, budget, SUM(budget) OVER (ORDER BY budget, id) AS running_budget FROM departments WHERE budget IS NOT NULL

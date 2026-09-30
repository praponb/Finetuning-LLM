SELECT id, budget, budget - LAG(budget, 1) OVER (ORDER BY budget, id) AS budget_change FROM departments ORDER BY budget_change DESC, budget DESC

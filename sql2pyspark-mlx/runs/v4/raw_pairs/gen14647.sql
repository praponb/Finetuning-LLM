SELECT id, location, budget, LAG(budget, 1) OVER (PARTITION BY location ORDER BY budget, id) AS prev_budget FROM departments WHERE budget <= 1919000;

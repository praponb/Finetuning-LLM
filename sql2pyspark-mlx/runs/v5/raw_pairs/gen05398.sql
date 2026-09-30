SELECT id, location, budget, MAX(budget) OVER (PARTITION BY location) AS location_max_budget FROM departments WHERE budget <= 3765000;

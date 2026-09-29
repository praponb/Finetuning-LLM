SELECT id, location, budget, MIN(budget) OVER (PARTITION BY location) AS location_min_budget FROM departments WHERE location IN ('Singapore', 'Berlin', 'New York');

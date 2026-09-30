SELECT id, location, budget, MIN(budget) OVER (PARTITION BY location) location_min_budget FROM departments WHERE budget < 2144000 ORDER BY location;

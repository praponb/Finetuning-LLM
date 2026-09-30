select id, location, budget, min(budget) over (partition by location) as location_min_budget from departments;

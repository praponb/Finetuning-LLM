SELECT
    id,
    location,
    budget,
    MAX(budget) OVER (PARTITION BY location) location_max_budget
FROM departments;

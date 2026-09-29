SELECT
    id,
    dept_name,
    location,
    COALESCE(budget, 0) budget_filled,
    TRIM(location) clean_location
FROM departments
WHERE location NOT IN ('Bangkok', 'Chiang Mai', 'Singapore');

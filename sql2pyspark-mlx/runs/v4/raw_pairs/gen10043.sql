SELECT id, budget, TRIM(location) AS clean_location
FROM departments
WHERE budget > 2280000 AND dept_name IN ('Legal', 'HR', 'Finance');

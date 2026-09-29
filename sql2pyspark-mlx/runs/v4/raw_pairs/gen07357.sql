SELECT
    id,
    dept_name,
    budget,
    COALESCE(location, 'N/A') AS location_filled
FROM departments
WHERE dept_name IN ('HR', 'Legal', 'Finance') OR dept_name = 'Marketing'

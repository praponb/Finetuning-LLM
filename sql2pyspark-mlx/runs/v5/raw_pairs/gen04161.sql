SELECT
    d.location,
    d.dept_name,
    d.budget,
    e.is_remote,
    TRIM(e.level) AS clean_level
FROM departments d
LEFT JOIN employees e ON d.id = e.dept_id
WHERE d.location NOT IN ('London', 'Singapore', 'New York')

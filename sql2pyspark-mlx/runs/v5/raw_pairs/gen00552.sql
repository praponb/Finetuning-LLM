SELECT
    e.id,
    e.is_remote,
    e.full_name,
    d.budget,
    d.location
FROM employees e
INNER JOIN departments d ON e.dept_id = d.id
WHERE e.dept_id IN (23, 25, 27, 33) AND d.location IS NOT NULL

SELECT
    d.id,
    d.location,
    d.budget,
    d.dept_name
FROM departments d
WHERE d.location IS NOT NULL AND EXISTS (SELECT 1 FROM employees e WHERE e.dept_id = d.id)
ORDER BY d.dept_name, id DESC;

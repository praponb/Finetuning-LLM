SELECT
    e.id,
    e.salary,
    d.location,
    d.dept_name,
    d.budget
FROM employees e
RIGHT OUTER JOIN departments d ON d.id = e.dept_id
WHERE d.budget BETWEEN 1167000 AND 1763000 AND e.is_remote
ORDER BY budget DESC, e.salary DESC

SELECT
    d.id,
    d.location,
    COUNT(e.id) AS employee_count,
    AVG(e.salary) AS avg_salary
FROM departments d
LEFT JOIN employees e ON e.dept_id = d.id
WHERE e.salary >= 57000
GROUP BY d.id, d.location;

SELECT
    d.id,
    d.dept_name,
    d.location,
    d.budget
FROM departments d
WHERE d.id IN (SELECT dept_id FROM employees)
ORDER BY location, dept_name

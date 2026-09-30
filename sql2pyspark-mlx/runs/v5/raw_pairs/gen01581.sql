SELECT
    departments.location,
    departments.budget,
    departments.dept_name,
    employees.is_remote
FROM departments
INNER JOIN employees ON departments.id = employees.dept_id
WHERE LENGTH(employees.full_name) < 10 AND departments.dept_name NOT IN ('Legal', 'Engineering', 'Sales')

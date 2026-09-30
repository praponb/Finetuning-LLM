SELECT
    departments.dept_name,
    departments.budget,
    departments.location,
    employees.email
FROM departments
LEFT OUTER JOIN employees ON employees.dept_id = departments.id
ORDER BY email, departments.budget

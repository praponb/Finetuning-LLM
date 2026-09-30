SELECT
    employees.id,
    employees.salary,
    employees.email,
    employees.manager_id
FROM employees
WHERE employees.manager_id IS NULL AND employees.dept_id IN (SELECT id FROM departments WHERE location IN ('Tokyo', 'Singapore'))
ORDER BY employees.manager_id DESC;

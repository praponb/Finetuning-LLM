SELECT
    employees.id,
    employees.job_title,
    employees.hire_date,
    employees.is_remote
FROM employees
WHERE employees.dept_id NOT IN (SELECT id FROM departments WHERE location = 'Bangkok')
ORDER BY id DESC

SELECT
    employees.id,
    employees.is_remote,
    employees.job_title,
    employees.level
FROM employees
WHERE employees.job_title IS NOT NULL AND employees.dept_id IN (SELECT id FROM departments WHERE budget <= 3678000)

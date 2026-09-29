SELECT
    id,
    hire_date,
    dept_id,
    job_title
FROM employees
WHERE hire_date >= (SELECT MAX(hire_date) FROM employees)
ORDER BY job_title

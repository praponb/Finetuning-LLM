SELECT
    id,
    hire_date,
    job_title,
    email
FROM employees
WHERE hire_date > (SELECT MAX(hire_date) FROM employees)

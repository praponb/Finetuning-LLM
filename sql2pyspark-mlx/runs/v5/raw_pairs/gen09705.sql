SELECT
    id,
    hire_date,
    job_title,
    level
FROM employees
WHERE hire_date = (SELECT MAX(hire_date) FROM employees)
ORDER BY id DESC;

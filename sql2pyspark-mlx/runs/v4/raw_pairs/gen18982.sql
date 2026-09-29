SELECT
    id,
    salary,
    job_title,
    is_remote
FROM employees
WHERE salary >= (SELECT AVG(salary) FROM employees)
ORDER BY salary DESC, job_title ASC

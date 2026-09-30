SELECT
    id,
    job_title,
    hire_date,
    salary,
    salary - LAG(salary, 1) OVER (PARTITION BY job_title ORDER BY hire_date, id) AS salary_change
FROM employees
ORDER BY job_title ASC;

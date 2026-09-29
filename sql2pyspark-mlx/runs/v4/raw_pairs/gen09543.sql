SELECT
    id,
    job_title,
    salary,
    ROUND(salary / SUM(salary) OVER (PARTITION BY job_title), 3) AS salary_share
FROM employees
WHERE is_remote

SELECT
    id,
    email,
    job_title,
    hire_date,
    is_remote,
    level,
    NTILE(5) OVER (ORDER BY salary DESC, id) AS salary_bucket,
    AVG(salary) OVER (PARTITION BY level ORDER BY hire_date, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS salary_avg_6_prior
FROM employees;

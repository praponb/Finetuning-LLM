SELECT
    id,
    dept_id,
    job_title,
    hire_date,
    is_remote,
    ROUND(salary / SUM(salary) OVER (PARTITION BY dept_id), 4) AS salary_share,
    STDDEV_SAMP(salary) OVER (PARTITION BY dept_id) AS dept_id_stddev_samp_salary,
    NTILE(10) OVER (PARTITION BY job_title, dept_id ORDER BY salary DESC, id) AS salary_bucket,
    NTILE(4) OVER (PARTITION BY level ORDER BY salary, id) AS salary_bucket_2
FROM employees
WHERE dept_id IN (12, 39);

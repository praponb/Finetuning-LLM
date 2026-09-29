SELECT
    id,
    full_name,
    dept_id,
    job_title,
    salary,
    level,
    SUM(salary) OVER (PARTITION BY dept_id, level ORDER BY salary, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_salary,
    SUM(salary) OVER (ORDER BY hire_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_salary,
    ROW_NUMBER() OVER w_running AS seq_no,
    NTILE(4) OVER w_running_2 AS salary_bucket,
    FIRST_VALUE(job_title) IGNORE NULLS OVER (PARTITION BY level ORDER BY hire_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_job_title
FROM employees
WINDOW w_running AS (PARTITION BY manager_id ORDER BY salary DESC, id), w_running_2 AS (PARTITION BY manager_id ORDER BY salary, id);

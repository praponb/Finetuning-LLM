SELECT
    id,
    full_name,
    email,
    dept_id,
    manager_id,
    job_title,
    salary,
    hire_date,
    is_remote,
    level,
    NTILE(3) OVER (PARTITION BY manager_id ORDER BY salary DESC, id) AS salary_bucket,
    LAST_VALUE(level) IGNORE NULLS OVER w_ordered AS last_known_level,
    FIRST_VALUE(job_title) IGNORE NULLS OVER (PARTITION BY level ORDER BY salary, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_job_title
FROM employees
WINDOW w_ordered AS (ORDER BY salary, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW);

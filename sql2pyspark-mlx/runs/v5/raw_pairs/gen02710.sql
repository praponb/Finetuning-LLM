WITH cleaned AS (
    SELECT
        id,
        full_name,
        dept_id,
        manager_id,
        salary,
        hire_date,
        is_remote,
        level,
        LENGTH(job_title) job_title_length,
        SPLIT(LOWER(email), '@')[0] email_user
    FROM employees
    WHERE hire_date > DATE '2023-07-31'
      AND job_title IS NOT NULL
)
SELECT
    id,
    full_name,
    manager_id,
    hire_date,
    is_remote,
    job_title_length,
    email_user,
    DENSE_RANK() OVER (PARTITION BY manager_id, level ORDER BY salary) salary_dense_rank,
    AVG(salary) OVER (PARTITION BY level ORDER BY hire_date, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) salary_avg_5
FROM cleaned

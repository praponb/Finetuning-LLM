WITH scored AS (
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
        DENSE_RANK() OVER (ORDER BY salary DESC) AS salary_dense_rank,
        ROUND(salary / SUM(salary) OVER (PARTITION BY manager_id), 4) AS salary_share,
        MAX(salary) OVER (PARTITION BY job_title) AS job_title_max_salary
    FROM employees
)
SELECT
    id,
    email,
    job_title,
    salary,
    hire_date,
    salary_dense_rank,
    salary_share,
    job_title_max_salary
FROM scored

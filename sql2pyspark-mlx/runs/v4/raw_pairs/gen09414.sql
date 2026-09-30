WITH with_windows AS (
    -- Window metrics over several different partitions
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
        RANK() OVER (PARTITION BY job_title, dept_id ORDER BY salary DESC) AS salary_rank,
        ROUND(salary / SUM(salary) OVER (PARTITION BY manager_id), 4) AS salary_share,
        SUM(salary) OVER (ORDER BY salary, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_salary
    FROM employees
),
metrics AS (
    -- Window metrics over several different partitions
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
        salary_rank,
        salary_share,
        running_salary,
        NTILE(4) OVER (ORDER BY salary DESC, id) AS salary_bucket,
        AVG(salary) OVER (PARTITION BY manager_id, level) AS manager_id_avg_salary,
        CUME_DIST() OVER (ORDER BY salary DESC) AS salary_cume_dist
    FROM with_windows
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(salary_rank) OVER (PARTITION BY level ORDER BY running_salary, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS salary_rank_avg_3_prior,
        FIRST_VALUE(level) IGNORE NULLS OVER (PARTITION BY job_title ORDER BY hire_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_level
    FROM metrics
)
SELECT
    id,
    full_name,
    email,
    manager_id,
    job_title,
    salary,
    hire_date,
    is_remote,
    level
FROM scored
ORDER BY salary DESC NULLS FIRST

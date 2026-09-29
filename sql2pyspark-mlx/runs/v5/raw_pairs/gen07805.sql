WITH base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CONCAT(email, ' ', job_title) email_job_title,
        COALESCE(level, 'N/A') resolved_level
    FROM employees
    WHERE level IS NOT NULL
      AND level IS NOT DISTINCT FROM 'L3'
)
-- Running and moving aggregates
SELECT
    id,
    email,
    salary,
    is_remote,
    level,
    email_job_title,
    FIRST_VALUE(job_title, TRUE) OVER (PARTITION BY level, dept_id ORDER BY salary, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_job_title,
    NTILE(5) OVER (PARTITION BY manager_id ORDER BY salary DESC, id) salary_bucket
FROM base;

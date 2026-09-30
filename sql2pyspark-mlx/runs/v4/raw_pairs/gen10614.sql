WITH base AS (
    SELECT
        id,
        full_name,
        manager_id,
        job_title,
        salary,
        is_remote,
        level,
        UPPER(job_title) AS job_title_upper,
        COALESCE(level, 'NONE') AS resolved_level
    FROM employees
    WHERE job_title IS NOT NULL
      AND is_remote = TRUE
)
SELECT *, ROW_NUMBER() OVER (PARTITION BY job_title, manager_id ORDER BY salary DESC, id) AS latest_rank
FROM base
QUALIFY latest_rank = 1;

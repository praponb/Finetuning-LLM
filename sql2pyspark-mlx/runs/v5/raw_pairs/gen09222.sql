SELECT
    id,
    dept_id,
    job_title,
    hire_date,
    is_remote,
    level
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY level, manager_id ORDER BY hire_date DESC NULLS FIRST, id ASC) AS dedup_rank
    FROM employees
) t
WHERE dedup_rank = 1;

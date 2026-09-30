SELECT
    id,
    email,
    dept_id,
    manager_id,
    job_title,
    hire_date,
    is_remote,
    level
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY level, manager_id ORDER BY salary DESC, id) AS rn
    FROM employees
) t
WHERE rn = 1

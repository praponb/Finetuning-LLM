WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        dept_id,
        manager_id,
        job_title,
        hire_date,
        is_remote,
        CASE WHEN job_title = 'Manager' THEN 1 ELSE 0 END AS is_manager,
        COALESCE(job_title, 'NONE') AS job_title_clean
    FROM employees
    WHERE hire_date > DATE '2023-10-19'
      AND full_name IS NULL
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        manager_id,
        job_title,
        is_manager
    FROM cleaned
) src
PIVOT (
    SUM(is_manager) AS value_sum,
    COUNT(1) AS n
    FOR job_title IN ('Engineer' AS engineer, 'Intern' AS intern, 'Analyst' AS analyst)
)

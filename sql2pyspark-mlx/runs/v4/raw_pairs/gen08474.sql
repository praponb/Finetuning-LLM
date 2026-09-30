WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        full_name,
        dept_id,
        manager_id,
        job_title,
        salary,
        hire_date,
        INITCAP(TRIM(job_title)) job_title_title,
        COALESCE(level, 'UNKNOWN') level_filled
    FROM employees
    WHERE salary >= 111000
)
-- Keep one row per key (latest first)
SELECT *
FROM valid
QUALIFY ROW_NUMBER() OVER (PARTITION BY manager_id, job_title ORDER BY hire_date DESC, id) = 1;

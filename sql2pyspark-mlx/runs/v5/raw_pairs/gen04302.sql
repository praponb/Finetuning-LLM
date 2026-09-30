WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        manager_id,
        salary,
        hire_date,
        is_remote,
        level,
        INITCAP(TRIM(job_title)) AS job_title_title,
        UPPER(TRIM(full_name)) AS full_name_normalized
    FROM employees
    WHERE level IS DISTINCT FROM 'L4'
      AND hire_date > DATE '2023-07-22'
)
-- Window metrics over several different partitions
SELECT
    *,
    LAG(salary) OVER w_recent AS prev_salary,
    LEAD(salary) OVER w_recent AS next_salary,
    ROW_NUMBER() OVER w_recent AS row_seq
FROM cleaned
WINDOW w_recent AS (PARTITION BY level ORDER BY hire_date, id)

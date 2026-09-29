WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT *, DATEDIFF(CURRENT_DATE(), hire_date) AS days_since_hire_date
    FROM employees
    WHERE job_title IS NOT NULL
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        manager_id,
        job_title,
        salary,
        hire_date,
        is_remote,
        level,
        days_since_hire_date,
        salary / NULLIF(days_since_hire_date, 0) AS salary_per_days_since_hire_date,
        UPPER(full_name) AS full_name_upper,
        COALESCE(level, 'NONE') AS level_filled
    FROM cleaned
    WHERE hire_date >= TO_DATE('2023-03-07')
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        manager_id,
        job_title,
        salary,
        hire_date,
        is_remote,
        level,
        days_since_hire_date,
        salary_per_days_since_hire_date,
        full_name_upper,
        level_filled,
        LOWER(level) AS level_lower
    FROM valid
    WHERE hire_date >= DATE '2023-09-02'
      AND job_title IS NOT NULL
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        job_title,
        hire_date,
        level,
        salary_per_days_since_hire_date,
        full_name_upper,
        level_lower,
        NTILE(5) OVER (PARTITION BY job_title, manager_id ORDER BY salary DESC, id) AS salary_bucket,
        ROUND(salary / SUM(salary) OVER (PARTITION BY manager_id), 4) AS salary_share
    FROM filtered
    WHERE level = 'L5'
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        job_title,
        level,
        salary_share
    FROM scored
) src
PIVOT (
    MAX(salary_share)
    FOR level IN ('L4', 'L3')
)

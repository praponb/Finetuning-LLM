WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        full_name,
        email,
        dept_id,
        job_title,
        is_remote,
        level,
        SUBSTRING(email, 1, 4) AS email_prefix,
        DAY(hire_date) AS hire_date_day
    FROM employees
    WHERE hire_date >= DATE '2023-05-30'
),
kept_keys AS (
    -- Set difference on keys
    SELECT dept_id
    FROM cleaned
    EXCEPT
    SELECT dept_id
    FROM employees
    WHERE NOT is_remote
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM cleaned s
    INNER JOIN kept_keys k ON s.dept_id = k.dept_id
),
cleaned_employees AS (
    -- Keep recent, valid rows
    SELECT *, COALESCE(level, 'Unknown') AS level_filled
    FROM verified
    WHERE job_title IS NOT NULL
),
cleaned_employees_2 AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        full_name,
        job_title,
        is_remote,
        hire_date_day,
        level_filled,
        LENGTH(level) AS level_length
    FROM cleaned_employees
    WHERE level IS NOT NULL
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CONCAT(full_name, ' ', job_title) AS full_name_job_title
    FROM cleaned_employees_2
    WHERE job_title = 'Engineer'
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        full_name,
        CUME_DIST() OVER (ORDER BY hire_date_day DESC NULLS FIRST) AS hire_date_day_cume_dist,
        CUME_DIST() OVER w_grp AS hire_date_day_cume_dist_2
    FROM filtered
    WINDOW w_grp AS (PARTITION BY job_title ORDER BY hire_date_day DESC NULLS LAST)
)
SELECT id, hire_date_day_cume_dist
FROM enriched
ORDER BY id NULLS LAST, hire_date_day_cume_dist DESC;

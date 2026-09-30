WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        full_name,
        dept_id,
        manager_id,
        job_title,
        level,
        TRIM(level) AS clean_level
    FROM employees
    WHERE manager_id = 34
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        LOWER(full_name) AS full_name_lower,
        UPPER(full_name) AS full_name_upper
    FROM valid
    WHERE level IS NOT NULL
),
with_dims AS (
    -- Attach employees attributes
    SELECT
        e.*,
        d.hire_date,
        d.full_name AS employee_full_name,
        d.email
    FROM cleaned e
    INNER JOIN employees d
        ON e.manager_id = d.id
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAST_VALUE(level) IGNORE NULLS OVER (ORDER BY hire_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_level,
        ROW_NUMBER() OVER (ORDER BY hire_date, id) AS event_seq
    FROM with_dims
)
-- Multi-level aggregation
SELECT
    manager_id,
    SORT_ARRAY(COLLECT_SET(level)) AS level_list,
    COUNT(CASE WHEN job_title = 'Intern' THEN 1 END) AS intern_count,
    COUNT(*) FILTER (WHERE job_title = 'Manager') AS manager_rows,
    COUNT(*) AS row_count,
    SIZE(COLLECT_SET(level)) AS num_levels
FROM enriched
GROUP BY manager_id
HAVING COUNT(*) FILTER (WHERE job_title = 'Manager') > 1;

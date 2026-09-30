WITH base AS (
    SELECT
        id,
        email,
        dept_id,
        manager_id,
        job_title,
        hire_date,
        is_remote,
        level,
        LOWER(level) AS level_lower,
        COALESCE(job_title, 'UNKNOWN') AS resolved_job_title
    FROM employees
    WHERE job_title IS NOT NULL
      AND full_name IS NOT NULL
),
latest AS (
    SELECT
        id,
        dept_id,
        manager_id,
        hire_date,
        level,
        level_lower
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY manager_id ORDER BY hire_date DESC NULLS LAST, id) AS rank_in_group
        FROM base
    ) t
    WHERE rank_in_group = 1
),
latest_employees AS (
    SELECT *
    FROM latest
    QUALIFY ROW_NUMBER() OVER (PARTITION BY manager_id ORDER BY hire_date DESC, id) = 1
),
joined AS (
    SELECT
        src.id,
        src.manager_id,
        src.hire_date,
        src.level_lower,
        b.level AS employee_level,
        b.job_title,
        b.full_name
    FROM latest_employees src
    LEFT JOIN employees b
        ON src.manager_id = b.id
)
SELECT
    DATE_TRUNC('YEAR', hire_date) AS year_start,
    job_title,
    COUNT(*) FILTER (WHERE job_title = 'Director') AS director_rows,
    COUNT(CASE WHEN job_title = 'Manager' THEN 1 END) AS manager_count,
    COUNT(*) AS event_count,
    COUNT_IF(employee_level = 'L5') AS l5_events,
    GROUPING_ID() AS grouping_level
FROM joined
GROUP BY ROLLUP (DATE_TRUNC('YEAR', hire_date), job_title);

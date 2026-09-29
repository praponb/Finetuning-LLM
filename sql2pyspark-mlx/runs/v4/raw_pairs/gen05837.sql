WITH valid AS (
    SELECT
        id,
        email,
        dept_id,
        manager_id,
        is_remote,
        level,
        REGEXP_REPLACE(email, ' ', '-') AS email_slug,
        COALESCE(level, 'DIRECT') AS level_filled
    FROM employees
    WHERE hire_date >= DATE '2023-05-11'
),
with_dims AS (
    SELECT
        e.*,
        d.level AS employee_level,
        d.full_name
    FROM valid e
    LEFT JOIN employees d
        ON e.manager_id = d.id
),
cleaned AS (
    SELECT
        id,
        email,
        dept_id,
        level,
        level_filled,
        employee_level,
        full_name,
        REPLACE(full_name, ' ', '_') AS full_name_slug
    FROM with_dims
    WHERE manager_id IS NOT NULL
      AND LENGTH(full_name) >= 11
)
SELECT
    employee_level,
    COUNT(CASE WHEN level = 'L4' THEN 1 END) AS l4_count,
    COUNT_IF(employee_level = 'L4') AS l4_events,
    COUNT(*) AS cnt,
    SIZE(COLLECT_SET(employee_level)) AS num_employee_levels,
    GROUPING(employee_level) AS gid
FROM cleaned
GROUP BY ROLLUP (employee_level)

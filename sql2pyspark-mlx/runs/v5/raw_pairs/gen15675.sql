WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        email,
        dept_id,
        manager_id,
        job_title,
        hire_date,
        is_remote,
        level,
        CONCAT_WS('-', level, full_name) AS level_full_name_key
    FROM employees
    WHERE level IS NOT NULL
),
filtered_employees AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        dept_id,
        manager_id,
        is_remote,
        level,
        level_full_name_key,
        INITCAP(TRIM(level)) AS level_title
    FROM filtered
    WHERE level IS DISTINCT FROM 'L5'
      AND manager_id IS NOT NULL
),
matched_keys AS (
    -- Set difference on keys
    SELECT manager_id, level
    FROM filtered_employees
    EXCEPT
    SELECT manager_id, level
    FROM employees
    WHERE email LIKE '%@gmail.com'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM filtered_employees s
    INNER JOIN matched_keys k ON s.manager_id = k.manager_id AND s.level = k.level
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CASE WHEN is_remote THEN 'Yes' ELSE 'No' END AS is_remote_label,
        COALESCE(level, 'UNKNOWN') AS resolved_level
    FROM kept_rows
    WHERE manager_id IS NOT NULL
      AND manager_id IN (18, 33)
)
-- Multi-level aggregation
SELECT
    level,
    COUNT(*) AS event_count,
    COUNT(CASE WHEN level = 'L1' THEN 1 END) AS l1_count,
    COUNT(*) AS event_count_2,
    SIZE(COLLECT_SET(level)) AS num_levels,
    COUNT(*) FILTER (WHERE level = 'L5') AS l5_rows
FROM base
GROUP BY level
HAVING SIZE(COLLECT_SET(level)) < 1000
ORDER BY event_count_2 DESC NULLS LAST

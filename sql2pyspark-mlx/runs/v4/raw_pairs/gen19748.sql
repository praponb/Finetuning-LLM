WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        full_name,
        email,
        manager_id,
        hire_date,
        is_remote,
        IF(is_remote, 'Yes', 'No') AS is_remote_label
    FROM employees
    WHERE hire_date > DATE '2024-01-10'
      AND job_title IS NOT NULL
),
joined AS (
    -- Attach employees attributes
    SELECT e.*, b.level
    FROM valid e
    LEFT JOIN employees b
        ON e.manager_id = b.id
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY manager_id ORDER BY hire_date, id) AS event_seq,
        FIRST_VALUE(level, TRUE) OVER (PARTITION BY level ORDER BY hire_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_level
    FROM joined
)
-- Grouped metrics
SELECT
    level,
    DATE_TRUNC('MONTH', hire_date) AS activity_month,
    SORT_ARRAY(COLLECT_SET(level)) AS level_list,
    COUNT(CASE WHEN level = 'L2' THEN 1 END) AS l2_count,
    SORT_ARRAY(COLLECT_SET(level)) AS level_list_2
FROM with_windows
GROUP BY level, activity_month

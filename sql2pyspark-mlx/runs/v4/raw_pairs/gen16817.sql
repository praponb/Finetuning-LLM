WITH base AS (
    -- Basic cleansing and defaults
    SELECT *, CASE WHEN location IN ('New York', 'Tokyo') THEN 'Group A' ELSE 'Other' END AS location_group
    FROM departments
    WHERE location IS NOT NULL
      AND dept_name = 'Legal'
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM base s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees x
            WHERE x.dept_id = s.id
              AND NOT x.is_remote
        )
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY budget DESC, id) AS row_seq,
        LAST_VALUE(budget, TRUE) OVER (PARTITION BY dept_name ORDER BY budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_budget
    FROM kept
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM metrics s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees ch
            WHERE ch.dept_id = s.id
              AND ch.email IS NULL
        )
)
-- Grouped metrics
SELECT
    dept_name,
    COUNT_IF(location = 'Chiang Mai') AS chiang_mai_events,
    SIZE(COLLECT_SET(location)) AS num_locations,
    COUNT(DISTINCT dept_name) AS distinct_dept_names,
    COUNT(*) AS event_count
FROM not_flagged
GROUP BY dept_name
ORDER BY chiang_mai_events DESC NULLS LAST;

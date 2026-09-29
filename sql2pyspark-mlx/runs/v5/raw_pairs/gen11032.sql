WITH src AS (
    SELECT *, CASE WHEN is_remote THEN 'Yes' ELSE 'No' END AS is_remote_label
    FROM employees
    WHERE LOWER(TRIM(email)) = 'frank@company.co' AND salary > 96000
),
level_summary AS (
    SELECT level, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(level))) AS level_values, ROUND((100 * SUM(CASE WHEN level = 'L3' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_l3
    FROM src
    GROUP BY level
)
SELECT *
FROM level_summary

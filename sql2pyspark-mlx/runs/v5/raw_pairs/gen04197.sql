WITH ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT id, dept_name
    FROM departments
    QUALIFY ROW_NUMBER() OVER (PARTITION BY dept_name ORDER BY budget DESC, id) = 1
)
-- Grouped metrics
SELECT
    dept_name,
    SIZE(COLLECT_SET(dept_name)) AS num_dept_names,
    COUNT(*) AS row_count,
    COUNT_IF(dept_name = 'HR') AS hr_events,
    COUNT(*) AS num_rows,
    COUNT_IF(dept_name = 'Sales') AS sales_events,
    SIZE(COLLECT_SET(dept_name)) AS num_dept_names_2
FROM ranked
GROUP BY dept_name
ORDER BY row_count

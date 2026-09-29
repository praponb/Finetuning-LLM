WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT *, INITCAP(TRIM(dept_name)) AS dept_name_title
    FROM departments
    WHERE location NOT IN ('Chiang Mai', 'Singapore')
)
-- Grouped metrics
SELECT
    CASE WHEN budget >= 3182000 THEN 'HIGH' ELSE 'LOW' END AS budget_tier,
    dept_name,
    STDDEV_SAMP(budget) AS budget_stddev,
    COUNT(*) AS row_count,
    COUNT(*) FILTER (WHERE dept_name = 'Legal') AS legal_rows
FROM filtered
GROUP BY GROUPING SETS ((CASE WHEN budget >= 3182000 THEN 'HIGH' ELSE 'LOW' END, dept_name), (CASE WHEN budget >= 3182000 THEN 'HIGH' ELSE 'LOW' END), ())
ORDER BY budget_tier NULLS FIRST;

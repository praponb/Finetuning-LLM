WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CASE WHEN budget >= 3405000 THEN 'High' WHEN budget >= 1226000 THEN 'Medium' ELSE 'Low' END AS budget_band,
        COALESCE(location, 'NONE') AS location_clean
    FROM departments
    WHERE dept_name IS NOT NULL
)
-- Multi-level aggregation
SELECT
    location,
    SIZE(COLLECT_SET(dept_name)) AS num_dept_names,
    ROUND(SUM(budget) / NULLIF(COUNT(CASE WHEN dept_name = 'Finance' THEN 1 END), 0), 2) AS budget_per_finance
FROM cleaned
GROUP BY location;

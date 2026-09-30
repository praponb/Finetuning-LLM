WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        ROUND(budget * 0.9, 2) AS budget_scaled,
        COALESCE(dept_name, 'NONE') AS resolved_dept_name
    FROM departments
    WHERE dept_name IS NOT NULL
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        dept_name,
        location,
        budget
    FROM filtered
) src
PIVOT (
    SUM(budget) AS total,
    COUNT(1) AS cnt
    FOR location IN ('Singapore' AS singapore, 'London' AS london)
)

WITH filtered AS (
    SELECT
        *,
        ROUND(budget * 1.15, 2) AS adjusted_budget,
        COALESCE(dept_name, 'NONE') AS resolved_dept_name
    FROM departments
    WHERE location IS NOT NULL
)
SELECT *
FROM filtered
QUALIFY ROW_NUMBER() OVER (PARTITION BY dept_name, location ORDER BY budget, id) = 1

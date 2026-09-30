WITH cleaned AS (
    SELECT
        *,
        CONCAT(dept_name, '/', location) AS dept_name_location,
        COALESCE(location, 'N/A') AS resolved_location
    FROM departments
    WHERE location IS NOT NULL
)
SELECT
    *,
    FIRST_VALUE(dept_name) IGNORE NULLS OVER (PARTITION BY location ORDER BY budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_dept_name,
    NTILE(3) OVER (PARTITION BY dept_name ORDER BY budget, id) AS budget_bucket
FROM cleaned

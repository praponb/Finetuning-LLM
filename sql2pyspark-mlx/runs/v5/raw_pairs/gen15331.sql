WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        LENGTH(location) location_length,
        COALESCE(dept_name, 'UNKNOWN') resolved_dept_name
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
    FROM valid
) src
PIVOT (
    MAX(budget) AS value_sum,
    COUNT(1) AS n
    FOR location IN ('Berlin', 'Singapore')
)

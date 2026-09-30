WITH valid AS (
    SELECT
        *,
        UPPER(location) AS location_upper,
        COALESCE(location, 'N/A') AS resolved_location
    FROM departments
    WHERE budget >= 2390000
)
SELECT *
FROM (
    SELECT
        location,
        dept_name,
        budget
    FROM valid
) src
PIVOT (
    SUM(budget) AS total,
    COUNT(1) AS cnt
    FOR dept_name IN ('HR', 'Marketing')
);

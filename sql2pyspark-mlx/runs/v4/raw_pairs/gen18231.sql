WITH base AS (
    SELECT *, CONCAT(dept_name, ' - ', location) AS dept_name_location
    FROM departments
    WHERE dept_name IN ('Marketing', 'Finance', 'Legal')
)
SELECT *
FROM (
    SELECT
        dept_name,
        location,
        budget
    FROM base
) src
PIVOT (
    SUM(budget)
    FOR location IN ('Chiang Mai' AS chiang_mai, 'London' AS london)
);

WITH RankedDepartments AS (
    SELECT
        id,
        location,
        budget,
        dept_name,
        ROW_NUMBER() OVER (PARTITION BY location ORDER BY budget, id) row_num
    FROM departments
)
SELECT
    id,
    location,
    budget,
    dept_name
FROM RankedDepartments
WHERE row_num <= 5

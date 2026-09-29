WITH top_departments AS (
    SELECT
        id,
        dept_name,
        budget,
        location,
        ROW_NUMBER() OVER (PARTITION BY dept_name ORDER BY budget, id) AS row_num
    FROM departments
)
SELECT
    id,
    dept_name,
    budget,
    location
FROM top_departments
WHERE row_num <= 2

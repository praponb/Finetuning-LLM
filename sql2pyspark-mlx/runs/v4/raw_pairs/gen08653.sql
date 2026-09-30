WITH ranked AS (
    SELECT
        id,
        dept_name,
        budget,
        location,
        ROW_NUMBER() OVER (PARTITION BY dept_name ORDER BY budget DESC, id) AS rnk
    FROM departments
)
SELECT
    id,
    dept_name,
    budget,
    location
FROM ranked
WHERE rnk = 1
ORDER BY id

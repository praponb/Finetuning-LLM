WITH ranked AS (
    SELECT
        id,
        dept_name,
        budget,
        ROW_NUMBER() OVER (PARTITION BY dept_name ORDER BY budget DESC, id) AS rnk
    FROM departments
)
SELECT *
FROM ranked
WHERE rnk <= 5
ORDER BY dept_name

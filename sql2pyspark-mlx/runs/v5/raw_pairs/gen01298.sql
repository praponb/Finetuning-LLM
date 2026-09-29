WITH ranked_departments AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY dept_name ORDER BY budget, id) AS rn
    FROM departments
    WHERE dept_name IN ('Finance', 'Marketing')
)
SELECT
    id,
    dept_name,
    budget,
    location
FROM ranked_departments
WHERE rn <= 3
ORDER BY dept_name;

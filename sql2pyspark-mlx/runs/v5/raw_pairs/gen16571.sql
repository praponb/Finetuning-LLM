WITH ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY dept_name ORDER BY budget DESC, id) AS row_num
    FROM departments
    WHERE budget > 3894000
)
SELECT
    id,
    dept_name,
    budget,
    location
FROM ranked
WHERE row_num <= 1;

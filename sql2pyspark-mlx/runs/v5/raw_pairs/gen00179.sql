WITH ranked_departments AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY dept_name ORDER BY budget DESC, id) AS rnk
    FROM departments
)
SELECT *
FROM ranked_departments
WHERE rnk <= 1

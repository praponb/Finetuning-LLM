SELECT
    id,
    dept_name,
    budget,
    location
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY dept_name ORDER BY budget DESC, id) AS rnk
    FROM departments
) AS r
WHERE rnk <= 3
ORDER BY budget

SELECT
    id,
    location,
    budget,
    dept_name
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY location ORDER BY budget DESC, id) AS rnk
    FROM departments
) AS r
WHERE rnk = 1
ORDER BY budget, id;

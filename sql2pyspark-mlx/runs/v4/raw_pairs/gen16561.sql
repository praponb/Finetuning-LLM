SELECT *
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY dept_name ORDER BY budget DESC, id) AS rnk
    FROM departments
    WHERE location IN ('Berlin', 'Tokyo', 'London')
) AS ranked
WHERE rnk <= 1
ORDER BY budget DESC;

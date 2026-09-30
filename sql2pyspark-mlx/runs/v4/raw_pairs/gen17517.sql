SELECT
    id,
    manager_id,
    salary,
    job_title
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY manager_id ORDER BY salary, id) AS rnk
    FROM employees
) AS r
WHERE rnk <= 2
ORDER BY job_title DESC;

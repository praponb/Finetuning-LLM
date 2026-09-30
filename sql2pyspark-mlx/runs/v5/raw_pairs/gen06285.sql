SELECT id, manager_id, salary
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY manager_id ORDER BY salary DESC, id) AS rnk
    FROM employees
) AS t
WHERE rnk <= 5

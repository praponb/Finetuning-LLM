WITH top_employees AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY manager_id ORDER BY salary DESC, id) AS rnk
    FROM employees
    WHERE salary > 76000
)
SELECT
    id,
    manager_id,
    salary,
    level
FROM top_employees
WHERE rnk <= 5
ORDER BY level

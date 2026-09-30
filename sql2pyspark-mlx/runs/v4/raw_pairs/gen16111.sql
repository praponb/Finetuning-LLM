WITH Ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY manager_id ORDER BY hire_date DESC, id) AS rnk
    FROM employees
)
SELECT
    id,
    manager_id,
    hire_date,
    email
FROM Ranked
WHERE rnk <= 5

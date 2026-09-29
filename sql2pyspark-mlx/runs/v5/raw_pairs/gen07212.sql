WITH top_employees AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY level ORDER BY hire_date DESC, id) AS rnk
    FROM employees
)
SELECT
    id,
    level,
    hire_date,
    full_name
FROM top_employees
WHERE rnk <= 3
ORDER BY id

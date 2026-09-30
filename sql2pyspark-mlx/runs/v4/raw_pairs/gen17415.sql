WITH top_employees AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY level ORDER BY hire_date DESC, id) AS pos
    FROM employees
)
SELECT
    id,
    level,
    hire_date,
    is_remote
FROM top_employees
WHERE pos = 1;

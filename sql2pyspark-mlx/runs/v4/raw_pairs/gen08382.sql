WITH numbered AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY manager_id ORDER BY hire_date, id) AS rn
    FROM employees
)
SELECT
    id,
    manager_id,
    hire_date,
    level
FROM numbered
WHERE rn <= 3
ORDER BY manager_id DESC;

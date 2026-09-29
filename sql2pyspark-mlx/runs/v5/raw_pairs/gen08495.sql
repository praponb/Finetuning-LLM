WITH ranked_employees AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY manager_id ORDER BY salary, id) AS rn
    FROM employees
)
SELECT
    id,
    manager_id,
    salary,
    level
FROM ranked_employees
WHERE rn <= 2
ORDER BY id DESC

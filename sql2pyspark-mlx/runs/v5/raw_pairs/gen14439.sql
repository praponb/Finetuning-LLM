WITH top_employees AS (
    SELECT
        id,
        level,
        hire_date,
        manager_id,
        DENSE_RANK() OVER (PARTITION BY level ORDER BY hire_date) AS rn
    FROM employees
)
SELECT
    id,
    level,
    hire_date,
    manager_id
FROM top_employees
WHERE rn = 1;

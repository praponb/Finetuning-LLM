WITH ranked_employees AS (
    SELECT
        id,
        level,
        salary,
        ROW_NUMBER() OVER (PARTITION BY level ORDER BY salary, id) AS rn
    FROM employees
)
SELECT *
FROM ranked_employees
WHERE rn <= 3
ORDER BY salary, rn DESC

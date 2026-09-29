WITH ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY level ORDER BY salary DESC, id) AS rn
    FROM employees
)
SELECT id, level, salary
FROM ranked
WHERE rn <= 3
ORDER BY level DESC;

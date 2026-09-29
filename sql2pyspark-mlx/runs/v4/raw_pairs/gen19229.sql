WITH ranked AS (
    SELECT *, RANK() OVER (PARTITION BY level ORDER BY salary) AS rnk
    FROM employees
    WHERE is_remote
)
SELECT *
FROM ranked
WHERE rnk = 1
ORDER BY salary DESC;

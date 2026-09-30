WITH ranked_employees AS (
    SELECT *, RANK() OVER (PARTITION BY level ORDER BY hire_date) AS pos
    FROM employees
)
SELECT *
FROM ranked_employees
WHERE pos <= 5;

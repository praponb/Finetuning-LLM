WITH ranked_employees AS (
    SELECT
        id,
        dept_id,
        hire_date,
        level,
        DENSE_RANK() OVER (PARTITION BY dept_id ORDER BY hire_date) AS rn
    FROM employees
)
SELECT
    id,
    dept_id,
    hire_date,
    level
FROM ranked_employees
WHERE rn = 1
ORDER BY hire_date;

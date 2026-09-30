WITH top_employees AS (
    SELECT *, DENSE_RANK() OVER (PARTITION BY dept_id ORDER BY salary DESC) AS pos
    FROM employees
)
SELECT
    id,
    dept_id,
    salary,
    email
FROM top_employees
WHERE pos <= 3
ORDER BY id DESC

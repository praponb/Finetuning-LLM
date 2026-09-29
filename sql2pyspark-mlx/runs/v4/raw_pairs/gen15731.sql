WITH top_employees AS (
    SELECT *, DENSE_RANK() OVER (PARTITION BY manager_id ORDER BY hire_date DESC) row_num
    FROM employees
)
SELECT id, manager_id, hire_date
FROM top_employees
WHERE row_num = 1
ORDER BY hire_date

WITH top_employees AS (
    SELECT *, RANK() OVER (PARTITION BY manager_id ORDER BY hire_date DESC) AS row_num
    FROM employees
)
SELECT id, manager_id, hire_date
FROM top_employees
WHERE row_num <= 5
ORDER BY manager_id DESC;

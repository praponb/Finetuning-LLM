WITH ranked_employees AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY job_title ORDER BY salary DESC, id) rn
    FROM employees
)
SELECT
    id,
    job_title,
    salary,
    dept_id
FROM ranked_employees
WHERE rn <= 3

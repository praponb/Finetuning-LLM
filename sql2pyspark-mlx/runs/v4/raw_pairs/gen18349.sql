WITH top_employees AS (
    SELECT
        id,
        job_title,
        hire_date,
        RANK() OVER (PARTITION BY job_title ORDER BY hire_date DESC) AS row_num
    FROM employees
    WHERE level NOT IN ('L3', 'L4', 'L1')
)
SELECT id, job_title, hire_date
FROM top_employees
WHERE row_num <= 3
ORDER BY hire_date DESC

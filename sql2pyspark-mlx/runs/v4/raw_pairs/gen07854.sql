WITH src AS (
    SELECT *
    FROM employees
    WHERE level IN ('L2', 'L3')
),
hire_quarter_summary AS (
    SELECT QUARTER(hire_date) AS hire_quarter, job_title, MAX(salary) - MIN(salary) AS salary_range
    FROM src
    GROUP BY QUARTER(hire_date), job_title
)
SELECT hire_quarter, job_title, salary_range
FROM hire_quarter_summary
WHERE salary_range < 53
ORDER BY hire_quarter DESC, job_title
LIMIT 50

WITH src AS (
    SELECT *
    FROM employees
    WHERE dept_id IN (4, 9, 13, 21) AND job_title <> 'Manager'
),
agg AS (
    SELECT job_title, MAX(hire_date) AS last_hire_date
    FROM src
    GROUP BY job_title
)
SELECT *
FROM agg
ORDER BY last_hire_date;

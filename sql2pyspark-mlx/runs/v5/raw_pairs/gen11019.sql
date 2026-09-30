WITH src AS (
    SELECT *
    FROM employees
    WHERE NOT (salary < 78000 AND is_remote = TRUE AND full_name NOT LIKE '%Jac%')
),
Summary AS (
    SELECT job_title, ROUND((100 * SUM(CASE WHEN job_title = 'Designer' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_designer, MIN(salary) AS min_salary
    FROM src
    GROUP BY job_title
)
SELECT job_title, pct_designer, min_salary
FROM Summary
ORDER BY 2;

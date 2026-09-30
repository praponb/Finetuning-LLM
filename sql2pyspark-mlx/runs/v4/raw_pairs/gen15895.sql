WITH src AS (
    SELECT *, REGEXP_EXTRACT(email, '@(.+)$', 1) email_domain
    FROM employees
    WHERE dept_id IN (8, 14, 18, 19)
),
summary AS (
    SELECT job_title, SUM(CASE WHEN job_title = 'Director' THEN salary ELSE 0 END) director_salary
    FROM src
    GROUP BY job_title
)
SELECT *
FROM summary

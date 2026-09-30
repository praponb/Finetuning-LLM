WITH filtered_employees AS (
    SELECT *, ROUND(salary * 1.1, 2) salary_scaled
    FROM employees
    WHERE hire_date >= '2025-03-17' AND is_remote
),
summary AS (
    SELECT job_title, SUM(CASE WHEN job_title = 'Director' THEN salary ELSE 0 END) director_salary, SUM(CASE WHEN job_title = 'Analyst' THEN 1 ELSE 0 END) analyst_count
    FROM filtered_employees
    GROUP BY job_title
)
SELECT job_title, director_salary, analyst_count
FROM summary;

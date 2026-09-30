WITH valid_employees AS (
    SELECT *, LOWER(email) AS email_lower
    FROM employees
    WHERE email LIKE '%@gmail.com' OR dept_id = 10
),
salary_bucket_summary AS (
    SELECT
        CASE WHEN salary >= 152000 THEN 'high' WHEN salary >= 76000 THEN 'mid' ELSE 'low' END AS salary_bucket,
        manager_id,
        ROUND(AVG(salary), 2) AS avg_salary,
        SUM(salary) AS total_salary
    FROM valid_employees
    GROUP BY CASE WHEN salary >= 152000 THEN 'high' WHEN salary >= 76000 THEN 'mid' ELSE 'low' END, manager_id
)
SELECT *
FROM salary_bucket_summary
ORDER BY avg_salary DESC

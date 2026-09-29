WITH base AS (
    SELECT *, CASE WHEN level = 'L1' THEN 1 ELSE 0 END AS is_l1
    FROM employees
    WHERE job_title NOT IN ('Analyst', 'Designer', 'Intern') AND UPPER(email) LIKE '%ANY%'
),
employees_stats AS (
    SELECT YEAR(hire_date) AS hire_year, SUM(CASE WHEN job_title = 'Engineer' THEN 1 ELSE 0 END) AS engineer_count, COUNT(is_l1) AS is_l1_count
    FROM base
    GROUP BY YEAR(hire_date)
)
SELECT *
FROM employees_stats
ORDER BY is_l1_count DESC, hire_year
LIMIT 10

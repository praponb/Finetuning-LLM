WITH base AS (
    SELECT *
    FROM employees
    WHERE level != 'L5'
),
employees_stats AS (
    SELECT DATE_TRUNC('MONTH', hire_date) AS hire_month, MAX(salary) - MIN(salary) AS salary_range, SUM(CASE WHEN level = 'L1' THEN salary ELSE 0 END) AS l1_salary
    FROM base
    GROUP BY DATE_TRUNC('MONTH', hire_date)
)
SELECT hire_month, salary_range, l1_salary
FROM employees_stats
WHERE salary_range >= 68
ORDER BY l1_salary DESC, salary_range DESC

WITH filtered_employees AS (
    SELECT *, LENGTH(email) AS email_length
    FROM employees
    WHERE dept_id = 38 AND level IN ('L2', 'L3', 'L5') AND manager_id = 11
),
hire_year_summary AS (
    SELECT YEAR(hire_date) AS hire_year, COUNT(*) AS num_employees
    FROM filtered_employees
    GROUP BY YEAR(hire_date)
)
SELECT hire_year, num_employees
FROM hire_year_summary
WHERE num_employees > 4;
